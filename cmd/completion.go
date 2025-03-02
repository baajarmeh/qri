package cmd

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/qri-io/ioes"
	"github.com/qri-io/qri/base/component"
	"github.com/qri-io/qri/lib"
	"github.com/spf13/cobra"
)

// NewAutocompleteCommand creates a new `qri complete` cobra command that prints autocomplete scripts
func NewAutocompleteCommand(f Factory, ioStreams ioes.IOStreams) *cobra.Command {
	o := &AutocompleteOptions{IOStreams: ioStreams}
	cfgOpt := ConfigOptions{IOStreams: ioStreams}
	cmd := &cobra.Command{
		Use:   "completion [bash|zsh]",
		Short: "generate shell auto-completion scripts",
		Long: `Completion generates auto-completion scripts for Bash or Zsh
which you can then source in your terminal or save to your profile to have it
run on each terminal session.`,
		Example: `  # load auto-completion for a single session
  $ source <(qri completion [bash|zsh])

  #configure your bash/zsh shell to load completions by adding to your bashrc/zshrc:
  # ~/.bashrc or ~/.zshrc

  $ source <(qri completion [bash|zsh])

  # alternatively you can pipe the output to a local script and
  # reference that as the source for faster loading.
`,
		Annotations: map[string]string{
			"group": "other",
		},
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.Run(cmd, args)
		},
		ValidArgs: []string{"bash", "zsh"},
	}

	configCompletion := &cobra.Command{
		Use:    "config [FIELD]",
		Hidden: true,
		Short:  "get configuration keys",
		Long:   `'qri completion config' is a util function for auto-completion of config keys, ignores private data`,
		Args:   cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := cfgOpt.CompleteConfig(f); err != nil {
				return err
			}
			return cfgOpt.GetConfig(args)
		},
	}

	structureCompletion := &cobra.Command{
		Use:       "structure [FIELD]",
		Hidden:    true,
		Short:     "get structure keys",
		Long:      `'qri completion structure' is a util function for auto-completion of structure keys`,
		Args:      cobra.MaximumNArgs(1),
		ValidArgs: component.DatasetFields,
		RunE: func(cmd *cobra.Command, args []string) error {
			for _, structureArg := range component.DatasetFields {
				fmt.Fprintln(ioStreams.Out, structureArg)
			}
			return nil
		},
	}

	cmd.AddCommand(configCompletion)
	cmd.AddCommand(structureCompletion)

	return cmd
}

// CompleteConfig adds any missing configuration that can only be added just before calling GetConfig
func (o *ConfigOptions) CompleteConfig(f Factory) (err error) {
	if o.inst, err = f.Instance(); err != nil {
		return
	}

	o.ProfileMethods, err = f.ProfileMethods()
	return
}

// GetConfig gets configuration keys based on a partial key supplied
func (o *ConfigOptions) GetConfig(args []string) (err error) {
	params := &lib.GetConfigParams{}
	if len(args) == 1 {
		params.Field = args[0]
	}

	ctx := context.TODO()

	data, err := o.inst.Config().GetConfigKeys(ctx, params)
	if err != nil {
		return err
	}

	fmt.Fprintln(o.Out, string(data))
	return
}

// AutocompleteOptions encapsulates completion options
type AutocompleteOptions struct {
	ioes.IOStreams
}

// Run executes the completion command
func (o *AutocompleteOptions) Run(cmd *cobra.Command, args []string) (err error) {
	if len(args) == 0 {
		return fmt.Errorf("shell not specified")
	}
	if len(args) > 1 {
		return fmt.Errorf("too many arguments, expected only the shell type")
	}
	if args[0] == "bash" {
		cmd.Parent().GenBashCompletion(o.Out)
	}
	if args[0] == "zsh" {
		zshBody := bytes.Buffer{}
		cmd.Parent().GenBashCompletion(&zshBody)
		io.WriteString(o.Out, zshHead)
		o.Out.Write(zshBody.Bytes())
		io.WriteString(o.Out, zshTail)
	}
	return nil
}

const (
	bashCompletionFunc = `
# arg completions

__qri_parse_list()
{
    local qri_output out
    if qri_output=$(qri list --format=simple --no-prompt --no-color 2>/dev/null); then
        echo "${qri_output}"
        return 1
    fi
    return 0
}

__qri_parse_search()
{
    local qri_output out
    if qri_output=$(qri search $cur --format=simple --no-prompt --no-color 2>/dev/null); then
        echo "${qri_output}"
        return 1
    fi
    return 0
}

__qri_parse_config()
{
    local qri_output out
    if qri_output=$(qri completion config $cur --no-prompt --no-color 2>/dev/null); then
        echo "${qri_output}"
        return 1
    fi
    return 0
}

__qri_parse_structure_args()
{
    local qri_output out
    if qri_output=$(qri completion structure --no-prompt --no-color 2>/dev/null); then
        echo "${qri_output}"
        return 1
    fi
    return 0
}

__qri_parse_peers()
{
    local qri_output out
    if qri_output=$(qri peers list --format=simple --no-prompt --no-color 2>/dev/null); then
        echo "${qri_output}"
        return 1
    fi
    return 0
}

__qri_join_completions()
{
    local q1=($1)
    local q2=($2)
    echo "${q1[*]} ${q2[*]}"
    return 1
}

__qri_suggest_completion()
{
	local res=($1)
    if test -z "${res}"; then
        return 0
    fi
    COMPREPLY=( $( compgen -W "${res[*]}" -- "$cur" ) )
    return 1
}

__qri_custom_func() {
    local out
    case ${last_command} in
        qri_checkout | qri_export | qri_log | qri_logbook | qri_publish | qri_remove | qri_rename | qri_render | qri_save | qri_stats | qri_use | qri_validate | qri_whatchanged | qri_workdir_link | qri_workdir_unlink)
            __qri_suggest_completion "$(__qri_parse_list)"
            return
            ;;
        qri_add | qri_fetch | qri_search)
        	__qri_suggest_completion "$(__qri_parse_search)"
        	return
        	;;
        qri_config_get | qri_config_set)
        	__qri_suggest_completion "$(__qri_parse_config)"
        	return
        	;;
        qri_get)
            local completions=$(__qri_join_completions "$(__qri_parse_structure_args)" "$(__qri_parse_list)")
        	__qri_suggest_completion "${completions}"
        	return
        	;;
        qri_peers_info | qri_peers_connect | qri_peers_disconnect)
            __qri_suggest_completion "$(__qri_parse_peers)"
            return
            ;;
        *)
            ;;
    esac
}

# flag completions

__qri_get_peer_flag_suggestions()
{
	__qri_suggest_completion "$(__qri_parse_peers)"
}
`

	zshHead = `# reference kubectl completion zsh

__qri_bash_source() {
	alias shopt=':'
	alias _expand=_bash_expand
	alias _complete=_bash_comp
	emulate -L sh
	setopt kshglob noshglob braceexpand

	source "$@"
}

__qri_type() {
	# -t is not supported by zsh
	if [ "$1" == "-t" ]; then
		shift

		# fake Bash 4 to disable "complete -o nospace". Instead
		# "compopt +-o nospace" is used in the code to toggle trailing
		# spaces. We don't support that, but leave trailing spaces on
		# all the time
		if [ "$1" = "__qri_compopt" ]; then
			echo builtin
			return 0
		fi
	fi
	type "$@"
}

__qri_compgen() {
	local completions w
	completions=( $(compgen "$@") ) || return $?

	# filter by given word as prefix
	while [[ "$1" = -* && "$1" != -- ]]; do
		shift
		shift
	done
	if [[ "$1" == -- ]]; then
		shift
	fi
	for w in "${completions[@]}"; do
		if [[ "${w}" = "$1"* ]]; then
			echo "${w}"
		fi
	done
}

__qri_compopt() {
	true # don't do anything. Not supported by bashcompinit in zsh
}

__qri_declare() {
	if [ "$1" == "-F" ]; then
		whence -w "$@"
	else
		builtin declare "$@"
	fi
}

__qri_ltrim_colon_completions()
{
	if [[ "$1" == *:* && "$COMP_WORDBREAKS" == *:* ]]; then
		# Remove colon-word prefix from COMPREPLY items
		local colon_word=${1%${1##*:}}
		local i=${#COMPREPLY[*]}
		while [[ $((--i)) -ge 0 ]]; do
			COMPREPLY[$i]=${COMPREPLY[$i]#"$colon_word"}
		done
	fi
}

__qri_get_comp_words_by_ref() {
	cur="${COMP_WORDS[COMP_CWORD]}"
	prev="${COMP_WORDS[${COMP_CWORD}-1]}"
	words=("${COMP_WORDS[@]}")
	cword=("${COMP_CWORD[@]}")
}

__qri_filedir() {
	# todo(arqu): This is a horrible hack to just return to normal 
	# path completion, ZSH has issues with correctly handling _filedir in this case.
	# Extension filtering is not supported right now
	return 1
	local RET OLD_IFS w qw

	__qri_debug "_filedir $@ cur=$cur"
	if [[ "$1" = \~* ]]; then
		# somehow does not work. Maybe, zsh does not call this at all
		eval echo "$1"
		return 0
	fi

	OLD_IFS="$IFS"
	IFS=$'\n'
	if [ "$1" = "-d" ]; then
		shift
		RET=( $(compgen -d) )
	else
		RET=( $(compgen -f) )
	fi
	IFS="$OLD_IFS"

	IFS="," __qri_debug "RET=${RET[@]} len=${#RET[@]}"

	for w in ${RET[@]}; do
		if [[ ! "${w}" = "${cur}"* ]]; then
			continue
		fi
		if eval "[[ \"\${w}\" = *.$1 || -d \"\${w}\" ]]"; then
			qw="$(__qri_quote "${w}")"
			if [ -d "${w}" ]; then
				COMPREPLY+=("${qw}/")
			else
				COMPREPLY+=("${qw}")
			fi
		fi
	done
}

__qri_quote() {
    if [[ $1 == \'* || $1 == \"* ]]; then
        # Leave out first character
        printf %q "${1:1}"
    else
    	printf %q "$1"
    fi
}

autoload -U +X bashcompinit && bashcompinit

# use word boundary patterns for BSD or GNU sed
LWORD='[[:<:]]'
RWORD='[[:>:]]'
if sed --help 2>&1 | grep -q GNU; then
	LWORD='\<'
	RWORD='\>'
fi

__qri_convert_bash_to_zsh() {
	sed \
	-e 's/declare -F/whence -w/' \
	-e 's/_get_comp_words_by_ref "\$@"/_get_comp_words_by_ref "\$*"/' \
	-e 's/local \([a-zA-Z0-9_]*\)=/local \1; \1=/' \
	-e 's/flags+=("\(--.*\)=")/flags+=("\1"); two_word_flags+=("\1")/' \
	-e 's/must_have_one_flag+=("\(--.*\)=")/must_have_one_flag+=("\1")/' \
	-e "s/${LWORD}_filedir${RWORD}/__qri_filedir/g" \
	-e "s/${LWORD}_get_comp_words_by_ref${RWORD}/__qri_get_comp_words_by_ref/g" \
	-e "s/${LWORD}__ltrim_colon_completions${RWORD}/__qri_ltrim_colon_completions/g" \
	-e "s/${LWORD}compgen${RWORD}/__qri_compgen/g" \
	-e "s/${LWORD}compopt${RWORD}/__qri_compopt/g" \
	-e "s/${LWORD}declare${RWORD}/builtin declare/g" \
	-e "s/\\\$(type${RWORD}/\$(__qri_type/g" \
	<<'BASH_COMPLETION_EOF'
`

	zshTail = `
BASH_COMPLETION_EOF
}

__qri_bash_source <(__qri_convert_bash_to_zsh)
`
)
