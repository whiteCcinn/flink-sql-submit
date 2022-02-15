#!/usr/bin/env bash
_mainScript_() {
    if [[ ${#ARGS[*]} != 2 ]];then
        if [[ ${#ARGS[*]} == 1 ]];then
            local _command=${ARGS[0]}
            case ${_command} in
                monitor | MONITOR)
                    _monitor
                ;;
                *)
                    printf "%s\n" "ERROR: Invalid command: ${_command}"
                    exit 1
                ;;
            esac
            return
        fi
        _usage_
        return
    fi
    local _command=${ARGS[0]}
    local _file=${ARGS[1]}

    if [[ -z ${_file} ]];then
         printf "%s\n" "ERROR: Invalid SQL File: ${_command}"
         exit 1
    fi

    case ${_command} in
        run | RUN)
            _run ${_file}
            ;;
        stop | STOP)
            _stop ${_file}
            ;;
        list | LIST)
            _list ${_file}
            ;;
        drop_table | DRPO_TABLE)
            _drop_table ${_file}
            ;;
        rebuild_run | REBUILD_RUN)
            _rebuild_run ${_file}
            ;;
        *)
            printf "%s\n" "ERROR: Invalid command: ${_command}"
            exit 1
            ;;
    esac
}
# end _mainScript_


_monitor() {
    yid=`find $BASH_SOURCE[0] -type f -name "yid"`;
    for i in $yid;do yid=`cat $i`;name=$(basename `dirname $i`);find `dirname $i` -type f -name '*.sql'| xargs -I {} cat {}| grep "SET[ \t'\"]\+pipeline.name" | sed -e "s/'//g" -e 's/"//g' | grep -Pio '(?<=\= )[^;]+' | sed 's/[ \t]//g' | awk -v name=$name -v yid=$yid '{print name,yid,$0}' | awk '{code=system("flink list -yid "$2" 2>/dev/null | grep -Ev INFO | grep "$3" > /dev/null");print "project:",$1", application_id: "$2", job_name:",$3", status:",code==0?"Running":"Failed";}' | grep Failed | xargs -I {} bash -c "msg=\"{}\";_send_metl_log \"\$msg\"";done
}

# 启动flink任务
_run() {
    local _file=$1
    local _yid=`_find_yid ${_file}`
    local _jobid=`_find_jobid ${_file}`
    local _pipelinename=`_find_pipelinename ${_file}`
    if [[ ! -z ${_jobid} ]];then
       local _jobid=`_find_jobid ${_file}`
       warning "任务已经启动，请勿重复提交 [yid: ${_yid}] [pipeline.name: ${_pipelinename}] [jobid: ${_jobid}]"
       return
    fi
    local _savepoint=`_find_savepoint ${_file}`
    if [[ -z ${_savepoint} ]] || [[ ${REHEAD_RUN} == true ]];then
        cmd="flink run -yid ${_yid} ${CLIENTPATH} job --sql-file ${_file}"
    else
        cmd="flink run -s ${_savepoint} -yid ${_yid} ${CLIENTPATH} job --sql-file ${_file}"
    fi
    header "${cmd}"
    ${cmd} 2>&1 | grep -Ev 'SLF4J|INFO|DEBUG'
    local _jobid=`_find_jobid ${_file}`

    if [[ ! -z ${_jobid} ]];then
        success "启动成功 [yid: ${_yid}] [pipeline.name: ${_pipelinename}] [jobid: ${_jobid}]"
    else
        warning "启动失败 [yid: ${_yid}] [pipeline.name: ${_pipelinename}]"
    fi
}

# 停止flink任务
_stop() {
    local _file=$1
    local _yid=`_find_yid ${_file}`
    local _jobid=`_find_jobid ${_file}`
    local _pipelinename=`_find_pipelinename ${_file}`
    local _savepoint_dir=`_find_savepoint_dir ${_file}`
    local _db=`_find_db ${_file}`
    local _savepoint_real_dir=`echo ${_savepoint_dir} | sed -e "s/{db}/${_db}/g" -e "s/{pipeline.name}/${_pipelinename}/g"`
    if [[ -z ${_jobid} ]];then
       warning "任务已经停止 [yid: ${_yid}] [pipeline.name: ${_pipelinename}]"
       return
    fi

    cmd="flink stop ${_jobid} -yid ${_yid} -p ${_savepoint_real_dir}"
    header "${cmd}"
    ${cmd} 2>/dev/null | grep -Ev 'SLF4J|INFO|DEBUG'
    if [[ $? -eq 0 ]];then
         local _savepoint=`_find_savepoint ${_file}`
        success "停止成功 [yid: ${_yid}] [pipeline.name: ${_pipelinename}] [savepoints: ${_savepoint}]"
    else
        warning "停止失败 [yid: ${_yid}] [pipeline.name: ${_pipelinename}]"
    fi
}

# 列出所有的列表
_list() {
    local _file=$1
    local _yid=`_find_yid ${_file}`
    cmd="flink list -yid ${_yid}"
    header "${cmd}"
    ${cmd} 2>/dev/null | grep -Ev 'SLF4J|INFO|DEBUG'
    if [[ $? -eq 0 ]];then
        success "列出成功 [yid: ${_yid}]"
    else
        warning "列出失败 [yid: ${_yid}]"
    fi
}

# 删除表
_drop_table() {
    local _file=$1
    local _db=`_find_db ${_file}`
    local _tables=`_find_tables ${_file}`
    cmd="flink run ${CLIENTPATH} hive-catalog drop_table --hive-conf ${HIVE_CONF_PATH} --db_name ${_db} --table_name ${_tables}"
    header "${cmd}"
    ${cmd} 2>/dev/null | grep -Ev 'SLF4J|INFO|DEBUG'
    if [[ $? -eq 0 ]];then
        success "删除表成功"
    else
        warning "删除表失败"
    fi
}

# 删除表，并且重跑
_rebuild_run() {
    _drop_table ${_file}
    _run ${_file}
}

_find_tables() {
    local _file=$1
    local _tables=`grep -Pio '(?<=CREATE TABLE )[^ ]+' ${_file} | sed -e 's/\`//g' | tr "\n" "," | sed -e 's/,$/\n/g'`
    echo ${_tables}
}

_find_savepoint_dir() {
    local _file=$1
    local _key="mc.execution.savepoint.dir"
    local _savepoint_dir=`_find_set ${_file} ${_key}`
    echo ${_savepoint_dir}
}

_find_savepoint() {
    local _file=$1
    local _savepoint_dir=`_find_savepoint_dir ${_file}`
    local _pipelinename=`_find_pipelinename ${_file}`
    local _db=`_find_db ${_file}`
    local _savepoint_real_dir=`echo ${_savepoint_dir} | sed -e "s/{db}/${_db}/g" -e "s/{pipeline.name}/${_pipelinename}/g"`
    local _savepoint=`hadoop fs -ls -t ${_savepoint_real_dir} 2>/dev/null | grep 'savepoints' | head -n 1 | awk '{print $8}'`
    echo ${_savepoint}
}

_find_pipelinename() {
    local _file=$1
    local _key="pipeline.name"
    local _pipelinename=`_find_set ${_file} ${_key}`
    echo ${_pipelinename}
}

_find_set() {
    local _file=$1
    local _key=$2
    local _value=`cat ${_file} | grep "SET[ \t'\"]\+${_key}" | sed -e "s/'//g" -e 's/"//g' | grep -Pio '(?<=\= )[^;]+' | sed 's/[ \t]//g'`
    echo ${_value}
}

_find_db() {
    local _file=$1
    local _db=`cat ${_file} | grep -Pio '(?<=USE )[^; ]+' | sed 's/[ \t]//g'`
    echo ${_db}
}

_find_jobid() {
    local _file=$1
    local _pipelinename=`_find_pipelinename ${_file}`
    local _yid=`_find_yid ${_file}`
    local _jobid=$(flink list -yid ${_yid} 2>/dev/null  | grep -Ev 'INFO|DEBUG' | awk -F ' : ' '{print $2,$3}' | awk '{print $1,$2}' | sed '/^$/d' | grep ${_pipelinename} | awk '{print $1}')
    echo ${_jobid}
}

_find_yid() {
    local _path=""
    local _file=$1
    local _level=`echo ${_file} | grep -Eo '/' | wc -l`
    while [[ ${_level} > 0 ]];do
        _file=`dirname ${_file}`
        _path=`find ${_file} -type f -name "yid"`
        if [[ ! -z ${_path} ]];then
            echo `cat ${_path} | sed -e '/^$/d' | head -n 1`
            return
        fi
        _level=$((${_level} - 1))
    done

    printf "%s\n" "ERROR: 找不到任何yid文件: $1"
    exit 1
}

_send_metl_log() {
  local metl_url='https://your-error-report.com'
  local message=$1
  local ip=`ifconfig | /bin/grep -Po '(?<=inet )[^ ]+' | head -n 1`
  local mtime=$(date +%s)
  local data="${message}"

  curl -XPOST -d"${data}" "${metl_url}"
  echo
}

export -f _send_metl_log

# ################################## Flags and defaults
# Required variables
LOGFILE="${HOME}/logs/$(basename "$0").log"
QUIET=false
LOGLEVEL=ERROR
VERBOSE=false
FORCE=false
DRYRUN=false
CLIENTPATH="/data/flink-stream/mc-flink-sql-submit-1.0-SNAPSHOT.jar"
HIVE_CONF_PATH="/opt/hadoopclient/Hive/config/"
# 是否不继承savepoint跑
REHEAD_RUN=false

declare -a ARGS=()

# Script specific

# ################################## Custom utility functions (Pasted from repository)

# ################################## Functions required for this template to work

_setColors_() {
    # DESC:
    #         Sets colors use for alerts.
    # ARGS:
    #         None
    # OUTS:
    #         None
    # USAGE:
    #         printf "%s\n" "${blue}Some text${reset}"

    if tput setaf 1 >/dev/null 2>&1; then
        bold=$(tput bold)
        underline=$(tput smul)
        reverse=$(tput rev)
        reset=$(tput sgr0)

        if [[ $(tput colors) -ge 256 ]] >/dev/null 2>&1; then
            white=$(tput setaf 231)
            blue=$(tput setaf 38)
            yellow=$(tput setaf 11)
            green=$(tput setaf 82)
            red=$(tput setaf 9)
            purple=$(tput setaf 171)
            gray=$(tput setaf 250)
        else
            white=$(tput setaf 7)
            blue=$(tput setaf 38)
            yellow=$(tput setaf 3)
            green=$(tput setaf 2)
            red=$(tput setaf 9)
            purple=$(tput setaf 13)
            gray=$(tput setaf 7)
        fi
    else
        bold="\033[4;37m"
        reset="\033[0m"
        underline="\033[4;37m"
        # shellcheck disable=SC2034
        reverse=""
        white="\033[0;37m"
        blue="\033[0;34m"
        yellow="\033[0;33m"
        green="\033[1;32m"
        red="\033[0;31m"
        purple="\033[0;35m"
        gray="\033[0;37m"
    fi
}

_alert_() {
    # DESC:
    #         Controls all printing of messages to log files and stdout.
    # ARGS:
    #         $1 (required) - The type of alert to print
    #                         (success, header, notice, dryrun, debug, warning, error,
    #                         fatal, info, input)
    #         $2 (required) - The message to be printed to stdout and/or a log file
    #         $3 (optional) - Pass '${LINENO}' to print the line number where the _alert_ was triggered
    # OUTS:
    #         stdout: The message is printed to stdout
    #         log file: The message is printed to a log file
    # USAGE:
    #         [_alertType] "[MESSAGE]" "${LINENO}"
    # NOTES:
    #         - The colors of each alert type are set in this function
    #         - For specified alert types, the funcstac will be printed

    local _color
    local _alertType="${1}"
    local _message="${2}"
    local _line="${3:-}" # Optional line number

    [[ $# -lt 2 ]] && fatal 'Missing required argument to _alert_'

    if [[ -n ${_line} && ${_alertType} =~ ^(fatal|error) && ${FUNCNAME[2]} != "_trapCleanup_" ]]; then
        _message="${_message} ${gray}(line: ${_line}) $(_printFuncStack_)"
    elif [[ -n ${_line} && ${FUNCNAME[2]} != "_trapCleanup_" ]]; then
        _message="${_message} ${gray}(line: ${_line})"
    elif [[ -z ${_line} && ${_alertType} =~ ^(fatal|error) && ${FUNCNAME[2]} != "_trapCleanup_" ]]; then
        _message="${_message} ${gray}$(_printFuncStack_)"
    fi

    if [[ ${_alertType} =~ ^(error|fatal) ]]; then
        _color="${bold}${red}"
    elif [ "${_alertType}" == "info" ]; then
        _color="${gray}"
    elif [ "${_alertType}" == "warning" ]; then
        _color="${red}"
    elif [ "${_alertType}" == "success" ]; then
        _color="${green}"
    elif [ "${_alertType}" == "debug" ]; then
        _color="${purple}"
    elif [ "${_alertType}" == "header" ]; then
        _color="${bold}${white}${underline}"
    elif [ "${_alertType}" == "notice" ]; then
        _color="${bold}"
    elif [ "${_alertType}" == "input" ]; then
        _color="${bold}${underline}"
    elif [ "${_alertType}" = "dryrun" ]; then
        _color="${blue}"
    else
        _color=""
    fi

    _writeToScreen_() {
        ("${QUIET}") && return 0 # Print to console when script is not 'quiet'
        [[ ${VERBOSE} == false && ${_alertType} =~ ^(debug|verbose) ]] && return 0

        if ! [[ -t 1 || -z ${TERM:-} ]]; then # Don't use colors on non-recognized terminals
            _color=""
            reset=""
        fi

        if [[ ${_alertType} == header ]]; then
            printf "${_color}%s${reset}\n" "${_message}"
        else
            printf "${_color}[%7s] %s${reset}\n" "${_alertType}" "${_message}"
        fi
    }
    _writeToScreen_

    _writeToLog_() {
        [[ ${_alertType} == "input" ]] && return 0
        [[ ${LOGLEVEL} =~ (off|OFF|Off) ]] && return 0
        if [ -z "${LOGFILE:-}" ]; then
            LOGFILE="$(pwd)/$(basename "$0").log"
        fi
        [ ! -d "$(dirname "${LOGFILE}")" ] && mkdir -p "$(dirname "${LOGFILE}")"
        [[ ! -f ${LOGFILE} ]] && touch "${LOGFILE}"

        # Don't use colors in logs
        local _cleanmessage
        _cleanmessage="$(printf "%s" "${_message}" | sed -E 's/(\x1b)?\[(([0-9]{1,2})(;[0-9]{1,3}){0,2})?[mGK]//g')"
        # Print message to log file
        printf "%s [%7s] %s %s\n" "$(date +"%b %d %R:%S")" "${_alertType}" "[$(/bin/hostname)]" "${_cleanmessage}" >>"${LOGFILE}"
    }

    # Write specified log level data to logfile
    case "${LOGLEVEL:-ERROR}" in
        ALL | all | All)
            _writeToLog_
            ;;
        DEBUG | debug | Debug)
            _writeToLog_
            ;;
        INFO | info | Info)
            if [[ ${_alertType} =~ ^(error|fatal|warning|info|notice|success) ]]; then
                _writeToLog_
            fi
            ;;
        NOTICE | notice | Notice)
            if [[ ${_alertType} =~ ^(error|fatal|warning|notice|success) ]]; then
                _writeToLog_
            fi
            ;;
        WARN | warn | Warn)
            if [[ ${_alertType} =~ ^(error|fatal|warning) ]]; then
                _writeToLog_
            fi
            ;;
        ERROR | error | Error)
            if [[ ${_alertType} =~ ^(error|fatal) ]]; then
                _writeToLog_
            fi
            ;;
        FATAL | fatal | Fatal)
            if [[ ${_alertType} =~ ^fatal ]]; then
                _writeToLog_
            fi
            ;;
        OFF | off)
            return 0
            ;;
        *)
            if [[ ${_alertType} =~ ^(error|fatal) ]]; then
                _writeToLog_
            fi
            ;;
    esac

} # /_alert_

error() { _alert_ error "${1}" "${2:-}"; }
warning() { _alert_ warning "${1}" "${2:-}"; }
notice() { _alert_ notice "${1}" "${2:-}"; }
info() { _alert_ info "${1}" "${2:-}"; }
success() { _alert_ success "${1}" "${2:-}"; }
dryrun() { _alert_ dryrun "${1}" "${2:-}"; }
input() { _alert_ input "${1}" "${2:-}"; }
header() { _alert_ header "${1}" "${2:-}"; }
debug() { _alert_ debug "${1}" "${2:-}"; }
fatal() {
    _alert_ fatal "${1}" "${2:-}"
    _safeExit_ "1"
}

_printFuncStack_() {
    # DESC:
    #         Prints the function stack in use. Used for debugging, and error reporting.
    # ARGS:
    #         None
    # OUTS:
    #         stdout: Prints [function]:[file]:[line]
    # NOTE:
    #         Does not print functions from the alert class
    local _i
    declare -a _funcStackResponse=()
    for ((_i = 1; _i < ${#BASH_SOURCE[@]}; _i++)); do
        case "${FUNCNAME[${_i}]}" in
            _alert_ | _trapCleanup_ | fatal | error | warning | notice | info | debug | dryrun | header | success)
                continue
                ;;
            *)
                _funcStackResponse+=("${FUNCNAME[${_i}]}:$(basename "${BASH_SOURCE[${_i}]}"):${BASH_LINENO[_i - 1]}")
                ;;
        esac

    done
    printf "( "
    printf %s "${_funcStackResponse[0]}"
    printf ' < %s' "${_funcStackResponse[@]:1}"
    printf ' )\n'
}

_safeExit_() {
    # DESC:
    #       Cleanup and exit from a script
    # ARGS:
    #       $1 (optional) - Exit code (defaults to 0)
    # OUTS:
    #       None

    if [[ -d ${SCRIPT_LOCK:-} ]]; then
        if command rm -rf "${SCRIPT_LOCK}"; then
            debug "Removing script lock"
        else
            warning "Script lock could not be removed. Try manually deleting ${yellow}'${SCRIPT_LOCK}'"
        fi
    fi

    if [[ -n ${TMP_DIR:-} && -d ${TMP_DIR:-} ]]; then
        if [[ ${1:-} == 1 && -n "$(ls "${TMP_DIR}")" ]]; then
            command rm -r "${TMP_DIR}"
        else
            command rm -r "${TMP_DIR}"
            debug "Removing temp directory"
        fi
    fi

    trap - INT TERM EXIT
    exit "${1:-0}"
}

_trapCleanup_() {
    # DESC:
    #         Log errors and cleanup from script when an error is trapped.  Called by 'trap'
    # ARGS:
    #         $1:  Line number where error was trapped
    #         $2:  Line number in function
    #         $3:  Command executing at the time of the trap
    #         $4:  Names of all shell functions currently in the execution call stack
    #         $5:  Scriptname
    #         $6:  $BASH_SOURCE
    # USAGE:
    #         trap '_trapCleanup_ ${LINENO} ${BASH_LINENO} "${BASH_COMMAND}" "${FUNCNAME[*]}" "${0}" "${BASH_SOURCE[0]}"' EXIT INT TERM SIGINT SIGQUIT SIGTERM ERR
    # OUTS:
    #         Exits script with error code 1

    local _line=${1:-} # LINENO
    local _linecallfunc=${2:-}
    local _command="${3:-}"
    local _funcstack="${4:-}"
    local _script="${5:-}"
    local _sourced="${6:-}"

    # Replace the cursor in-case 'tput civis' has been used
    tput cnorm

    if declare -f "fatal" &>/dev/null && declare -f "_printFuncStack_" &>/dev/null; then

        _funcstack="'$(printf "%s" "${_funcstack}" | sed -E 's/ / < /g')'"

        if [[ ${_script##*/} == "${_sourced##*/}" ]]; then
            fatal "${7:-} command: '${_command}' (line: ${_line}) [func: $(_printFuncStack_)]"
        else
            fatal "${7:-} command: '${_command}' (func: ${_funcstack} called at line ${_linecallfunc} of '${_script##*/}') (line: ${_line} of '${_sourced##*/}') "
        fi
    else
        printf "%s\n" "Fatal error trapped. Exiting..."
    fi

    if declare -f _safeExit_ &>/dev/null; then
        _safeExit_ 1
    else
        exit 1
    fi
}

_makeTempDir_() {
    # DESC:
    #         Creates a temp directory to house temporary files
    # ARGS:
    #         $1 (Optional) - First characters/word of directory name
    # OUTS:
    #         Sets $TMP_DIR variable to the path of the temp directory
    # USAGE:
    #         _makeTempDir_ "$(basename "$0")"

    [ -d "${TMP_DIR:-}" ] && return 0

    if [ -n "${1:-}" ]; then
        TMP_DIR="${TMPDIR:-/tmp/}${1}.${RANDOM}.${RANDOM}.$$"
    else
        TMP_DIR="${TMPDIR:-/tmp/}$(basename "$0").${RANDOM}.${RANDOM}.${RANDOM}.$$"
    fi
    (umask 077 && mkdir "${TMP_DIR}") || {
        fatal "Could not create temporary directory! Exiting."
    }
    debug "\$TMP_DIR=${TMP_DIR}"
}

# shellcheck disable=SC2120
_acquireScriptLock_() {
    # DESC:
    #         Acquire script lock to prevent running the same script a second time before the
    #         first instance exits
    # ARGS:
    #         $1 (optional) - Scope of script execution lock (system or user)
    # OUTS:
    #         exports $SCRIPT_LOCK - Path to the directory indicating we have the script lock
    #         Exits script if lock cannot be acquired
    # NOTE:
    #         If the lock was acquired it's automatically released in _safeExit_()

    local _lockDir
    if [[ ${1:-} == 'system' ]]; then
        _lockDir="${TMPDIR:-/tmp/}$(basename "$0").lock"
    else
        _lockDir="${TMPDIR:-/tmp/}$(basename "$0").${UID}.lock"
    fi

    if command mkdir "${_lockDir}" 2>/dev/null; then
        readonly SCRIPT_LOCK="${_lockDir}"
        debug "Acquired script lock: ${yellow}${SCRIPT_LOCK}${purple}"
    else
        if declare -f "_safeExit_" &>/dev/null; then
            error "Unable to acquire script lock: ${yellow}${_lockDir}${red}"
            fatal "If you trust the script isn't running, delete the lock dir"
        else
            printf "%s\n" "ERROR: Could not acquire script lock. If you trust the script isn't running, delete: ${_lockDir}"
            exit 1
        fi

    fi
}

_setPATH_() {
    # DESC:
    #         Add directories to $PATH so script can find executables
    # ARGS:
    #         $@ - One or more paths
    # OPTS:
    #         -x - Fail if directories are not found
    # OUTS:
    #         0: Success
    #         1: Failure
    #         Adds items to $PATH
    # USAGE:
    #         _setPATH_ "/usr/local/bin" "${HOME}/bin" "$(npm bin)"

    [[ $# == 0 ]] && fatal "Missing required argument to ${FUNCNAME[0]}"

    local opt
    local OPTIND=1
    local _failIfNotFound=false

    while getopts ":xX" opt; do
        case ${opt} in
            x | X) _failIfNotFound=true ;;
            *)
                {
                    error "Unrecognized option '${1}' passed to _backupFile_" "${LINENO}"
                    return 1
                }
                ;;
        esac
    done
    shift $((OPTIND - 1))

    local _newPath

    for _newPath in "$@"; do
        if [ -d "${_newPath}" ]; then
            if ! printf "%s" "${PATH}" | grep -Eq "(^|:)${_newPath}($|:)"; then
                if PATH="${_newPath}:${PATH}"; then
                    debug "Added '${_newPath}' to PATH"
                else
                    debug "'${_newPath}' already in PATH"
                fi
            else
                debug "_setPATH_: '${_newPath}' already exists in PATH"
            fi
        else
            debug "_setPATH_: can not find: ${_newPath}"
            if [[ ${_failIfNotFound} == true ]]; then
                return 1
            fi
            continue
        fi
    done
    return 0
}

_parseOptions_() {
    # DESC:
    #					Iterates through options passed to script and sets variables. Will break -ab into -a -b
    #         when needed and --foo=bar into --foo bar
    # ARGS:
    #					$@ from command line
    # OUTS:
    #					Sets array 'ARGS' containing all arguments passed to script that were not parsed as options
    # USAGE:
    #					_parseOptions_ "$@"

    # Iterate over options
    local _optstring=h
    declare -a _options
    local _c
    local i
    while (($#)); do
        case $1 in
            # If option is of type -ab
            -[!-]?*)
                # Loop over each character starting with the second
                for ((i = 1; i < ${#1}; i++)); do
                    _c=${1:i:1}
                    _options+=("-${_c}") # Add current char to options
                    # If option takes a required argument, and it's not the last char make
                    # the rest of the string its argument
                    if [[ ${_optstring} == *"${_c}:"* && -n ${1:i+1} ]]; then
                        _options+=("${1:i+1}")
                        break
                    fi
                done
                ;;
            # If option is of type --foo=bar
            --?*=*) _options+=("${1%%=*}" "${1#*=}") ;;
            # add --endopts for --
            --) _options+=(--endopts) ;;
            # Otherwise, nothing special
            *) _options+=("$1") ;;
        esac
        shift
    done
    set -- "${_options[@]:-}"
    unset _options

    # Read the options and set stuff
    # shellcheck disable=SC2034
    while [[ ${1:-} == -?* ]]; do
        case $1 in
            # Custom options
             -c | --client)
                shift
                CLIENTPATH="${1}"
                ;;
            # Common options
            -h | --help)
                _usage_
                _safeExit_
                ;;
            --loglevel)
                shift
                LOGLEVEL=${1}
                ;;
            --logfile)
                shift
                LOGFILE="${1}"
                ;;
            -n | --dryrun) DRYRUN=true ;;
            -v | --verbose) VERBOSE=true ;;
            -q | --quiet) QUIET=true ;;
            --force) FORCE=true ;;
            -f ) REHEAD_RUN=true;;
            --endopts)
                shift
                break
                ;;
            *)
                if declare -f _safeExit_ &>/dev/null; then
                    fatal "invalid option: $1"
                else
                    printf "%s\n" "ERROR: Invalid option: $1"
                    exit 1
                fi
                ;;
        esac
        shift
    done

    if [[ -z ${*} || ${*} == null ]]; then
        ARGS=()
    else
        ARGS+=("$@") # Store the remaining user input as arguments.
    fi
}

_usage_() {
    cat <<USAGE_TEXT

  ${bold}$(basename "$0") [OPTION] <COMMAND>${reset}

  Flink流计算SQL-Client的执行脚本

  ${bold}Command:${reset}
    ${green}run          [FILE]            运行${reset}
    ${red}stop         [FILE]            停止${reset}
    list         [FILE]            列出FILE所在yid下的所有job任务列表
    drop_table   [FILE]            删除所有表
    rebuild_run  [FILE]            删除所有表，然后重跑(继承savepoint）
    monitor                        监控job

  ${bold}Command-Common-Options:${reset}
    -c, --clientpath  [LEVEL]    flink-sql-submit.jar路径  (Default is '/data/tmp/mc-flink-sql-submit-1.0-SNAPSHOT.jar')
    -f   是否强制运行，忽略以往savepoint

  ${bold}Common-Options:${reset}
    -h, --help              Display this help and exit
    --loglevel [LEVEL]      One of: FATAL, ERROR, WARN, INFO, NOTICE, DEBUG, ALL, OFF
                            (Default is 'ERROR')
    --logfile [FILE]        Full PATH to logfile.  (Default is '${HOME}/logs/$(basename "$0").log')
    -n, --dryrun            Non-destructive. Makes no permanent changes.
    -q, --quiet             Quiet (no output)
    -v, --verbose           Output more information. (Items echoed to 'verbose')
    --force                 Skip all user interaction.  Implied 'Yes' to all actions.
USAGE_TEXT
}

trap '_trapCleanup_ ${LINENO} ${BASH_LINENO} "${BASH_COMMAND}" "${FUNCNAME[*]}" "${0}" "${BASH_SOURCE[0]}"' EXIT INT TERM SIGINT SIGQUIT SIGTERM

set -o errtrace
set -o errexit
#set -o pipefail

# Confirm we have BASH greater than v4
[ "${BASH_VERSINFO:-0}" -ge 4 ] || {
    printf "%s\n" "ERROR: BASH_VERSINFO is '${BASH_VERSINFO:-0}'.  This script requires BASH v4 or greater."
    exit 1
}

# Make `for f in *.txt` work when `*.txt` matches zero files
shopt -s nullglob globstar

# Set IFS to preferred implementation
IFS=$' \n\t'

# Run in debug mode
# set -o xtrace

# Initialize color constants
_setColors_

# Disallow expansion of unset variables
set -o nounset

# Force arguments when invoking the script
# [[ $# -eq 0 ]] && _parseOptions_ "-h"

# Parse arguments passed to script
_parseOptions_ "$@"

# Create a temp directory '$TMP_DIR'
# _makeTempDir_ "$(basename "$0")"

# Acquire script lock
_acquireScriptLock_

# Run the main logic script
_mainScript_

# Exit cleanly
_safeExit_