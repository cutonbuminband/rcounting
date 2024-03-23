import logging
import os
from collections import defaultdict
from functools import reduce
from pathlib import Path

from rcounting.reddit_interface import subreddit

printer = logging.getLogger(__name__)
default_filename = os.path.dirname(__file__) / Path("aliases.txt")


def read_aliases(filename=default_filename):
    with open(filename, "r", encoding="utf8") as f:
        alias_list = f.readlines()
    alias_list = [line.replace(" ", "").strip().split(",") for line in alias_list]
    alias_dict = {}
    for aliases in alias_list:
        for alias in aliases:
            alias_dict[alias] = aliases[0]
    return alias_dict


def normalise_aliases(alias_dict):
    aliases = {}
    for alias in alias_dict:
        aliases[alias.lower()] = alias_dict[alias]
    return aliases


def to_csv(mydict):
    s = ""
    for key in sorted(mydict.keys(), key=lambda x: x.lower()):
        vals = mydict[key]
        s += ",".join([key] + sorted(set(vals) - set([key]), key=lambda x: x.lower())) + "\n"
    return s


def read_aliases_from_wiki(page):
    wiki_page = subreddit.wiki[page]
    document = wiki_page.content_md.replace("\r\n", "\n").replace("*", "")
    lines = document.split("\n")
    aliases = [y for line in lines if len(y := line.split(":")) == 2]
    new_aliases = {}
    for canonical_name, current_aliases in aliases:
        alias_list = current_aliases.replace(" ", "").split(",")
        new_aliases[canonical_name] = alias_list
    return new_aliases


def sync_aliases(alias_dict, wiki_aliases):
    """The meat of the logic for merging two alias dicts.

    The first is assumed to be in the form key -> canonical name, while the
    second is in the form canonical_name -> list of aliases.

    Returns a dict of the form canonical_name -> list of aliases, with the following merge logic:

    - If a collection of aliases is present in both dictionaries,
      merge the two and use the merged collection.
    - If a collection of aliases is only present in one of the two dictionaries,
      add that directly to the result.
    - If the canonical name is different between the stored aliases and the wiki aliases,
      use the wiki name.
    """
    reverse_dict = defaultdict(list)
    for key in alias_dict:
        reverse_dict[alias_dict[key]].append(key)
    updated_aliases = {}
    overlapping_aliases = set()
    for key in wiki_aliases:
        target_keys = [key.lower()] + [x.lower() for x in wiki_aliases[key]]
        target_keys = {alias_dict[x] for x in target_keys if x in alias_dict}
        overlapping_aliases |= target_keys
        old_aliases = reduce(
            lambda x, y: x | set(y), [reverse_dict[x] for x in target_keys], set()
        )
        new_aliases = list(set(wiki_aliases[key]))
        lowercase = [x.lower() for x in new_aliases]
        for old_alias in old_aliases:
            if old_alias.lower() not in lowercase:
                new_aliases.append(old_alias)
                lowercase.append(old_alias.lower())
        updated_aliases[key] = new_aliases
    updated_aliases = updated_aliases | {
        k: v for k, v in reverse_dict.items() if k not in overlapping_aliases
    }
    return updated_aliases


def write_aliases(aliases, filename=default_filename):
    csv_text = to_csv(aliases)
    with open(filename, "w", encoding="utf8") as f:
        f.write(csv_text)


def update_aliases(filename=default_filename):
    """The main interface to the alias updates. Reads the stored alias file,
    loads the one from the wiki page, merges the two and overwrites the old
    alias file. The internal alias dict is then updated, so that `apply_alias`
    will work as expected right after calling this function.
    """

    alias_dict = read_aliases(filename=filename)
    wiki_aliases = read_aliases_from_wiki("aliases")
    try:
        updated_aliases = sync_aliases(alias_dict, wiki_aliases)
    # Pylint complains about a broad except here which is fair enough; the idea
    # is that at any signs of error we abort the operation and just keep going
    # with the old aliases dict. That seems safer than trying to recover and
    # ending up in an unknown state.
    except Exception as e:  # pylint: disable=broad-except
        printer.error("Something went wrong while fetching aliases from wiki page. Aborting")
        printer.error(e, exc_info=True)
        return
    write_aliases(updated_aliases, filename=filename)
    # Pylint complains about this global. For now, it seems to be the easiest
    # way to share state between this function and `apply alias`.
    global _alias_dict  # pylint: disable=global-statement
    _alias_dict = normalise_aliases(read_aliases())


_alias_dict = normalise_aliases(read_aliases())


def apply_alias(username):
    return _alias_dict[username.lower()] if username.lower() in _alias_dict else username


mods = [
    "Z3F",
    "949paintball",
    "zhige",
    "atomicimploder",
    "ekjp",
    "TheNitromeFan",
    "davidjl123",
    "rschaosid",
    "KingCaspianX",
    "Urbul",
    "Zaajdaeon",
]


def is_mod(username):
    return username in mods


ignored_counters = [
    "LuckyNumber-Bot",
    "CountingStatsBot",
    "CountingHelper",
    "WikiSummarizerBot",
    "InactiveUserDetector",
    "alphabet_order_bot",
    "exclaim_bot",
]
ignored_counters = [x.lower() for x in ignored_counters]
banned_counters = ["[deleted]", "Franciscouzo", "None"]


def is_ignored_counter(username):
    return username.lower() in ignored_counters


def is_banned_counter(username):
    return username in banned_counters
