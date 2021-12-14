from pathlib import Path

alias_list = open(Path('input/prefs/aliases.txt'), 'r').readlines()
alias_list = [x.strip().split(',') for x in alias_list]
alias_dict = {}
for aliases in alias_list:
    for alias in aliases:
        alias_dict[alias] = aliases[0]


def apply_alias(username):
    return alias_dict[username] if username in alias_dict else username


mods = ['Z3F',
        '949paintball',
        'zhige',
        'atomicimploder',
        'ekjp',
        'TheNitromeFan',
        'davidjl123',
        'rschaosid',
        'KingCaspianX',
        'Urbul',
        'Zaajdaeon']


def is_mod(username):
    return username in mods


ignored_counters = ['[deleted]', 'Franciscouzo']


def is_ignored_counter(username):
    return username in ignored_counters
