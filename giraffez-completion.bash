#!/usr/bin/env bash

__parse_yaml() {
   local prefix=$2
   local s='[[:space:]]*' w='[a-zA-Z0-9_]*' fs=$(echo @|tr @ '\034')
   sed -ne "s|^\($s\):|\1|" \
        -e "s|^\($s\)\($w\)$s:$s[\"']\(.*\)[\"']$s\$|\1$fs\2$fs\3|p" \
        -e "s|^\($s\)\($w\)$s:$s\(.*\)$s\$|\1$fs\2$fs\3|p"  $1 |
   awk -F$fs '{
      indent = length($1)/2;
      vname[indent] = $2;
      for (i in vname) {if (i > indent) {delete vname[i]}}
      if (length($3) > 0) {
         vn=""; for (i=0; i<indent; i++) {vn=(vn)(vname[i])(".")}
         #printf("%s%s ", vn, $2);
         printf("%s%s\n", vn, $2);
      }
   }'
}

__global_opts()
{
    local cur="${COMP_WORDS[COMP_CWORD]}"
    local prev="${COMP_WORDS[COMP_CWORD-1]}"
    local opts="--dsn --conf --key"
    if [[ ${cur} == -* ]]; then
        COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
        printf %s "$opts"
        return 0
    fi
}

_giraffez_cmd()
{
    local cur="${COMP_WORDS[COMP_CWORD]}"
    local prev="${COMP_WORDS[COMP_CWORD-1]}"
    local opts="--table-output"
    if [[ ${cur} == -* ]]; then
        COMPREPLY=( $(compgen -W '$(__global_opts) ${opts}' -- ${cur}) )
    fi
    compopt -o default
}

_giraffez_config()
{
    local cur="${COMP_WORDS[COMP_CWORD]}"
    local prev="${COMP_WORDS[COMP_CWORD-1]}"
    local opts="--no-newline --init --get --set --list --unset --unlock"
    case "$prev" in
    --init)
        _giraffez_config_init
        return 0
        ;;
    --list | -l)
        _giraffez_config_list
        return 0
        ;;
    --get | --set | --unset)
        _giraffez_config_get_set
        return 0
        ;;
    --unlock)
        _giraffez_config_unlock
        return 0
        ;;
    *)
        if [ ${COMP_CWORD} = 2 ]; then
            COMPREPLY=( $(compgen -W '${opts[@]}' -- ${cur}) )
        fi
        ;;
    esac
    return 0
}

_giraffez_config_init()
{
    return 0
}

_giraffez_config_list()
{
    local cur="${COMP_WORDS[COMP_CWORD]}"
    local prev="${COMP_WORDS[COMP_CWORD-1]}"
    local opts="-d --decrypt"
    if [[ ${cur} == -* ]]; then
        COMPREPLY=( $(compgen -W '$(__global_opts) ${opts}' -- ${cur}) )
    fi
    return 0
}

_giraffez_config_get_set()
{

    local prev="${COMP_WORDS[COMP_CWORD-1]}"
    local cur=${COMP_WORDS[COMP_CWORD]}
    local opts=$(__parse_yaml $HOME/.girafferc | while read line; do
        if [[ $line == $cur* ]]; then
            rem=${line/$cur/}
            if [[ $rem == *.* ]]; then
                echo "${cur}$(echo $rem | cut -d'.' -f-1)."
            else
                echo "$line "
            fi
        fi
    done | uniq)


    COMPREPLY=($(compgen -o nospace -W "$opts" -- $cur))

    if [ ${#COMPREPLY[@]} = 1 ] && [[ ${COMPREPLY[0]} != *. ]]; then
        COMPREPLY=$(printf %q%s "$COMPREPLY" " ")
    fi
    compopt -o nospace
    return 0
}

_giraffez_config_unlock()
{

    local prev="${COMP_WORDS[COMP_CWORD-1]}"
    local cur=${COMP_WORDS[COMP_CWORD]}
    local opts=$(__parse_yaml $HOME/.girafferc | while read line; do
        if [[ $line == connections.$cur*locked ]]; then
            rem=${line/connections\.$cur/}
            echo "${cur}$(echo $rem | cut -d'.' -f-1) "
        fi
    done | uniq)


    COMPREPLY=($(compgen -o nospace -W "$opts" -- $cur))

    if [ ${#COMPREPLY[@]} = 1 ] && [[ ${COMPREPLY[0]} != *. ]]; then
        COMPREPLY=$(printf %q%s "$COMPREPLY" " ")
    fi
    compopt -o nospace
    return 0
}

_giraffez_export()
{
    local cur="${COMP_WORDS[COMP_CWORD]}"
    local prev="${COMP_WORDS[COMP_CWORD-1]}"
    local opts="--encoding --delimiter --null --no-header --archive --gzip"
    if [[ ${cur} == -* ]]; then
        COMPREPLY=( $(compgen -W '$(__global_opts) ${opts}' -- ${cur}) )
    fi
    compopt -o default
}

_giraffez_fmt()
{
    local cur="${COMP_WORDS[COMP_CWORD]}"
    local prev="${COMP_WORDS[COMP_CWORD-1]}"
    local opts="--delimiter --null --head --count"
    if [[ ${cur} == -* ]]; then
        COMPREPLY=( $(compgen -W '$(__global_opts) ${opts}' -- ${cur}) )
    fi
    compopt -o default
}

_giraffez_load()
{
    local cur="${COMP_WORDS[COMP_CWORD]}"
    local prev="${COMP_WORDS[COMP_CWORD-1]}"
    local opts="--encoding --delimiter --null --date-conversion --disable-date-conversion"
    if [[ ${cur} == -* ]]; then
        COMPREPLY=( $(compgen -W '$(__global_opts) ${opts}' -- ${cur}) )
    fi
    compopt -o default
}

_giraffez_mload()
{
    local cur="${COMP_WORDS[COMP_CWORD]}"
    local prev="${COMP_WORDS[COMP_CWORD-1]}"
    local opts="--encoding --delimiter --null --drop-all"
    if [[ ${cur} == -* ]]; then
        COMPREPLY=( $(compgen -W '$(__global_opts) ${opts}' -- ${cur}) )
    fi
    compopt -o default
}

_giraffez_run()
{
    local cur="${COMP_WORDS[COMP_CWORD]}"
    local prev="${COMP_WORDS[COMP_CWORD-1]}"
    local opts=""
    if [[ ${cur} == -* ]]; then
        COMPREPLY=( $(compgen -W '$(__global_opts) ${opts}' -- ${cur}) )
    fi
    compopt -o default
}

_giraffez_secret()
{

    local prev="${COMP_WORDS[COMP_CWORD-1]}"
    local cur=${COMP_WORDS[COMP_CWORD]}
    local opts=$(__parse_yaml $HOME/.girafferc | while read line; do
        if [[ $line == $cur* ]]; then
            rem=${line/$cur/}
            if [[ $rem == *.* ]]; then
                echo "${cur}$(echo $rem | cut -d'.' -f-1)."
            else
                echo "$line "
            fi
        fi
    done | uniq)


    COMPREPLY=($(compgen -o nospace -W "$opts" -- $cur))

    if [ ${#COMPREPLY[@]} = 1 ] && [[ ${COMPREPLY[0]} != *. ]]; then
        COMPREPLY=$(printf %q%s "$COMPREPLY" " ")
    fi
    compopt -o nospace
    return 0
}

_giraffez_shell()
{
    local cur="${COMP_WORDS[COMP_CWORD]}"
    local prev="${COMP_WORDS[COMP_CWORD-1]}"
    local opts="--table-output"
    if [[ ${cur} == -* ]]; then
        COMPREPLY=( $(compgen -W '$(__global_opts) ${opts}' -- ${cur}) )
    fi
}

_giraffez()
{
    local cmds=(cmd config export fmt load mload run secret shell)
    local opts="--help --version"
    local cmd="${COMP_WORDS[1]}"
    local cur="${COMP_WORDS[COMP_CWORD]}"

    case "$cmd" in
    cmd)
        _giraffez_cmd
        return 0
        ;;
    config)
        _giraffez_config
        return 0
        ;;
    export)
        _giraffez_export
        return 0
        ;;
    fmt)
        _giraffez_fmt
        return 0
        ;;
    load)
        _giraffez_load
        return 0
        ;;
    mload)
        _giraffez_mload
        return 0
        ;;
    run)
        _giraffez_run
        return 0
        ;;
    secret)
        _giraffez_secret
        return 0
        ;;
    shell)
        _giraffez_shell
        return 0
        ;;
    -*)
        if [ $COMP_CWORD = 1 ]; then
            COMPREPLY=( $(compgen -W '$(__global_opts) ${opts}' -- ${cur}) )
        fi
        return 0
        ;;
    *)
        if [ $COMP_CWORD = 1 ]; then
            COMPREPLY=( $(compgen -W '${cmds[@]}' -- ${cur}) )
        fi
        ;;
    esac

    return 0
}

complete -F _giraffez giraffez
