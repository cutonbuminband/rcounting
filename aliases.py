from pathlib import Path

alias_list = open(Path('input/prefs/aliases.txt'), 'r').readlines()
alias_list = [x.strip().split(',') for x in alias_list]
alias_dict = {}
for aliases in alias_list:
    for alias in aliases:
        alias_dict[alias] = aliases[0]


def apply_alias(username):
    return alias_dict[username] if username in alias_dict else username
