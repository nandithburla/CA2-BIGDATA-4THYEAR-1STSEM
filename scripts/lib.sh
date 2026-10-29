#!/usr/bin/env bash
# =====================================================================================================
# Shared helpers for run_q1.sh ... run_q5.sh   (this file is SOURCED, not executed)
#   * reads optional config/ca2.env
#   * never deletes input data; asks before removing an existing HDFS output directory
#   * never invents results: everything printed/saved comes from real commands or real job counters
# =====================================================================================================

CA2_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# ---- configuration (config/ca2.env overrides the defaults) -------------------------------------------
if [[ -f "$CA2_ROOT/config/ca2.env" ]]; then
  # shellcheck disable=SC1091
  source "$CA2_ROOT/config/ca2.env"
fi
: "${DATA_DIR:=$HOME/ca2_data/CA2_Dataset}"
: "${HDFS_USER:=${USER:-$(whoami)}}"
: "${HDFS_BASE:=/user/$HDFS_USER/ca2}"
: "${REDUCERS:=4}"
: "${BUILD_TOOL:=mvn}"
: "${ASSUME_YES:=0}"
: "${PAUSE_FOR_SCREENSHOTS:=0}"
: "${RM_UI:=http://localhost:8088}"
: "${HISTORY_UI:=http://localhost:19888}"
: "${NN_UI:=http://localhost:9870}"
: "${SKIP_BUILD:=0}"
: "${SKIP_UPLOAD:=0}"

if [[ -t 1 ]]; then
  C_B=$'\033[1m'; C_G=$'\033[32m'; C_Y=$'\033[33m'; C_R=$'\033[31m'; C_C=$'\033[36m'; C_0=$'\033[0m'
else
  C_B=""; C_G=""; C_Y=""; C_R=""; C_C=""; C_0=""
fi

say()  { printf '%s[%s]%s %s\n' "$C_C" "$(date +%H:%M:%S)" "$C_0" "$*"; }
ok()   { printf '%s[ OK ]%s %s\n' "$C_G" "$C_0" "$*"; }
warn() { printf '%s[WARN]%s %s\n' "$C_Y" "$C_0" "$*" >&2; }
die()  { printf '%s[FAIL]%s %s\n' "$C_R" "$C_0" "$*" >&2; exit 1; }
step() { echo; printf '%s================ %s ================%s\n' "$C_B" "$*" "$C_0"; }

require_cmd() { command -v "$1" >/dev/null 2>&1 || die "Required command not found on PATH: $1"; }

# ---- common command line flags ------------------------------------------------------------------------
# usage inside a script:   parse_common_flags "$@"   -> sets ASSUME_YES / PAUSE_FOR_SCREENSHOTS / SKIP_BUILD / SKIP_UPLOAD
#                          remaining arguments are left in the array EXTRA_ARGS
parse_common_flags() {
  EXTRA_ARGS=()
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -y|--yes)        ASSUME_YES=1 ;;
      --pause)         PAUSE_FOR_SCREENSHOTS=1 ;;
      --no-build)      SKIP_BUILD=1 ;;
      --skip-upload)   SKIP_UPLOAD=1 ;;
      -h|--help)       usage; exit 0 ;;
      *)               EXTRA_ARGS+=("$1") ;;
    esac
    shift
  done
}

ask_yes_no() {
  local q="$1" a
  if [[ "$ASSUME_YES" == "1" ]]; then say "$q -> yes (--yes / ASSUME_YES=1)"; return 0; fi
  if [[ ! -t 0 ]]; then die "'$q' needs your confirmation but stdin is not a terminal. Run interactively or pass --yes."; fi
  read -r -p "$q [y/N] " a
  [[ "$a" =~ ^[Yy]$ ]]
}

# ---- environment --------------------------------------------------------------------------------------
check_env() {
  step "Checking the Hadoop environment"
  require_cmd java; require_cmd hadoop; require_cmd hdfs
  say "$(hadoop version 2>/dev/null | head -1)"
  say "$(java -version 2>&1 | grep -v 'Picked up' | head -1)"
  if [[ "${CA2_SKIP_ENV_CHECK:-0}" == "1" ]]; then warn "CA2_SKIP_ENV_CHECK=1 -> daemon checks skipped"; return 0; fi
  hdfs dfs -ls / >/dev/null 2>&1 || die "HDFS is not reachable. Start it with: start-dfs.sh   (then re-run)"
  ok "HDFS reachable"
  if command -v yarn >/dev/null 2>&1; then
    if yarn node -list 2>/dev/null | grep -q 'RUNNING'; then ok "YARN ResourceManager reachable and a NodeManager is RUNNING"
    else die "No RUNNING YARN NodeManager. Start YARN with: start-yarn.sh   (then re-run)"; fi
  else
    warn "'yarn' command not found - cannot verify the ResourceManager"
  fi
  if command -v jps >/dev/null 2>&1; then
    local procs; procs="$(jps 2>/dev/null || true)"
    echo "$procs" | grep -q JobHistoryServer || warn "JobHistoryServer is not running - start it for the JobHistory screenshots:  mapred --daemon start historyserver"
  fi
  say "Web UIs: ResourceManager $RM_UI   JobHistory $HISTORY_UI   NameNode $NN_UI"
}

# ---- local output housekeeping (never silently overwrite earlier results) --------------------------------
archive_previous() {            # archive_previous DIR   -> moves old files to DIR/_previous/<timestamp>/
  local d="$1" ts n
  mkdir -p "$d"
  n=$(find "$d" -mindepth 1 -maxdepth 1 ! -name '_previous' ! -name '.gitkeep' | wc -l)
  if [[ "$n" -gt 0 ]]; then
    ts="$(date +%Y%m%d_%H%M%S)"
    mkdir -p "$d/_previous/$ts"
    find "$d" -mindepth 1 -maxdepth 1 ! -name '_previous' ! -name '.gitkeep' -exec mv {} "$d/_previous/$ts"/ \;
    say "Previous contents of $d moved to $d/_previous/$ts"
  fi
}

# ---- HDFS helpers -------------------------------------------------------------------------------------
hdfs_exists() { hdfs dfs -test -e "$1" 2>/dev/null; }

confirm_remove_hdfs() {         # confirm_remove_hdfs PATH   -> asks before deleting an EXISTING HDFS path
  local p="$1"
  if hdfs_exists "$p"; then
    warn "HDFS path already exists: $p"
    hdfs dfs -ls "$p" 2>/dev/null | head -8 || true
    ask_yes_no "Delete $p and re-create it with the new results?" || die "Not deleting $p - aborting so nothing is overwritten. Choose another HDFS_BASE in config/ca2.env or delete it yourself."
    hdfs dfs -rm -r -f -skipTrash "$p" >/dev/null
    ok "removed $p"
  fi
}

hdfs_stage_input() {            # hdfs_stage_input HDFS_DIR LOCAL_FILE...   (copies only when missing/different; never deletes without asking)
  local hdir="$1"; shift
  local files=("$@") local_bytes hdfs_bytes
  [[ ${#files[@]} -gt 0 ]] || die "No local input files given for $hdir"
  for f in "${files[@]}"; do [[ -f "$f" ]] || die "Local input file not found: $f (set DATA_DIR in config/ca2.env; now DATA_DIR=$DATA_DIR)"; done
  local_bytes=$(cat "${files[@]}" | wc -c)
  if hdfs_exists "$hdir"; then
    hdfs_bytes=$(hdfs dfs -du -s "$hdir" 2>/dev/null | awk '{print $1}')
    if [[ "$hdfs_bytes" == "$local_bytes" ]]; then ok "Input already in HDFS: $hdir ($hdfs_bytes bytes, identical size) - not touched"; return 0; fi
    warn "$hdir exists but holds $hdfs_bytes bytes; the local files total $local_bytes bytes"
    ask_yes_no "Replace the contents of $hdir with the local files?" || die "Keeping the existing HDFS input is not possible: sizes differ. Aborting."
    hdfs dfs -rm -r -f -skipTrash "$hdir" >/dev/null
  fi
  hdfs dfs -mkdir -p "$hdir"
  hdfs dfs -put "${files[@]}" "$hdir"/
  ok "Uploaded ${#files[@]} file(s) to $hdir"
  hdfs dfs -ls -h "$hdir"
}

# ---- build --------------------------------------------------------------------------------------------
build_with_javac() {            # build_with_javac MODULE_DIR JAR_PATH MAIN_CLASS
  local mod="$1" jar="$2" main="$3" cp tmp
  require_cmd javac; require_cmd jar
  cp="${CA2_HADOOP_CP:-$(hadoop classpath)}"
  tmp="$(mktemp -d)"
  # shellcheck disable=SC2046
  javac --release 11 -encoding UTF-8 -cp "$cp" -d "$tmp" $(find "$mod/src" -name '*.java') || { rm -rf "$tmp"; return 1; }
  jar cf "$jar" -C "$tmp" . || { rm -rf "$tmp"; return 1; }
  rm -rf "$tmp"
}

build_module() {                # build_module MODULE_DIR_NAME JAR_BASENAME MAIN_CLASS
  local name="$1" base="$2" main="$3"
  local mod="$CA2_ROOT/$name" jar="$CA2_ROOT/$name/jar/$base.jar" blog="$CA2_ROOT/$name/logs/build.log"
  step "Building $name"
  mkdir -p "$mod/jar" "$mod/logs"
  if [[ "$SKIP_BUILD" == "1" && -f "$jar" ]]; then say "--no-build: using existing $jar"; return 0; fi
  if [[ "$BUILD_TOOL" == "mvn" ]] && command -v mvn >/dev/null 2>&1; then
    say "mvn -q -DskipTests -pl $name -am package      (log: $blog)"
    if (cd "$CA2_ROOT" && mvn -q -DskipTests -pl "$name" -am package) >"$blog" 2>&1 && [[ -f "$mod/target/$base.jar" ]]; then
      cp "$mod/target/$base.jar" "$jar"; ok "Built $jar with Maven"; return 0
    fi
    warn "Maven build failed (see $blog). Falling back to javac against 'hadoop classpath'."
  fi
  say "javac --release 11 -cp \$(hadoop classpath) ...   (log: $blog)"
  build_with_javac "$mod" "$jar" "$main" >>"$blog" 2>&1 || { tail -20 "$blog" >&2; die "Build failed - see $blog"; }
  ok "Built $jar with javac"
}

# ---- running jobs and reading their counters ------------------------------------------------------------
run_logged() {                  # run_logged LOGFILE COMMAND...   (prints the exact command, tees the output, returns the exit code)
  local log="$1"; shift
  say "COMMAND: $*"
  local rc
  set +e
  "$@" 2>&1 | tee "$log"
  rc=${PIPESTATUS[0]}
  set -e
  return "$rc"
}

assert_ran_on_yarn() {          # assert_ran_on_yarn LOGFILE
  local log="$1"
  if grep -q 'job_local' "$log"; then
    die "The job ran with the LocalJobRunner (job_local...), NOT on YARN. Set mapreduce.framework.name=yarn in mapred-site.xml and restart YARN; the assignment requires YARN."
  fi
  local apps; apps="$(grep -o 'application_[0-9]*_[0-9]*' "$log" | sort -u | tr '\n' ' ' || true)"
  [[ -n "$apps" ]] && ok "YARN application id(s): $apps" || warn "No application_ id found in $log"
}

counter() {                     # counter TSV_FILE COUNTER_NAME   -> value (0 if absent)
  awk -F'\t' -v n="$2" '$3==n {s+=$4; f=1} END {print f ? s : 0}' "$1" 2>/dev/null || echo 0
}

print_counters() {              # print_counters TSV_FILE [group-substring]
  local f="$1" g="${2:-}"
  [[ -f "$f" ]] || { warn "no counter file $f"; return 0; }
  awk -F'\t' -v g="$g" 'NR>1 && (g=="" || index($2,g)>0) {printf "  %-55s %s\n", $3, $4}' "$f"
}

yarn_apps() {
  command -v yarn >/dev/null 2>&1 && yarn application -list -appStates ALL 2>/dev/null | tail -n +2 | head -30 || true
}

shot() {                        # shot ID "what to capture"   -> screenshot instruction (never creates a screenshot itself)
  local id="$1"; shift
  echo
  printf '%s>>> SCREENSHOT %s%s  %s\n' "$C_Y" "$id" "$C_0" "$*"
  if [[ "$PAUSE_FOR_SCREENSHOTS" == "1" && -t 0 ]]; then read -r -p "    (take it now, then press ENTER to continue) " _; fi
}

sample_file() {                 # sample_file SRC DEST [lines]  -> first N lines (default 20) of a local file
  head -n "${3:-20}" "$1" > "$2"
}

banner() {
  echo "${C_B}#######################################################################################${C_0}"
  echo "${C_B}# $*${C_0}"
  echo "${C_B}#######################################################################################${C_0}"
}
