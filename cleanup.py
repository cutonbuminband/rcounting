from pathlib import Path
import pandas as pd
from psaw import PushshiftAPI

api = PushshiftAPI()


def find_missing_ids(df):
    """Use the reddit apis to find missing comment and submission ids
    """
    # Find the children of the relevant comments,
    children = df.loc[df[df['comment_id'].isna()].index + 1, ['comment_id', 'submission_id']]
    # Look for their parents using psaw
    psaw_comments = api.search_comments(ids=children['comment_id'], metadata='true', limit=0)
    parents = {x.id: x.parent_id[3:] for x in psaw_comments
               if x.parent_id[1] == '1'}
    # Check if there are any comments that psaw missed, and look for those using praw
    missing_children = set(children['comment_id']) - set(parents.keys())
    for missing_child in missing_children:
        comment = reddit.comment(missing_child)
        if not comment.is_root:
            parent = comment.parent()
            parents[missing_child] = parent.id
    parents = pd.Series(parents, name='parent_id')

    # combine parents and children into one table, with comment_id as the join
    # key, keeping the index in place.
    combined = pd.merge(children, parents, left_on='comment_id', right_index=True)
    # Then shift the index up to match the index of the broken parent records
    combined.set_index(combined.index - 1, inplace=True)
    # Finally overwrite the broken data with new data
    df.loc[combined.index, 'comment_id'] = combined['parent_id']
    df.loc[combined.index, 'submission_id'] = combined['submission_id']
    return df


if __name__ == "__main__":
    import argparse
    from reddit_interface import reddit

    parser = argparse.ArgumentParser(description="Add missing reddit ids to csv file")
    parser.add_argument("input", help="The input csv file")
    parser.add_argument("-o", "--output",
                        help="The output csv file. If not specified, the input is overwritten")
    args = parser.parse_args()

    in_file = args.input
    out_file = args.output
    if out_file is None:
        out_file = in_file
    columns = ['count', 'username', 'timestamp', 'comment_id', 'submission_id']
    df = pd.read_csv(Path(in_file), names=columns)
    df = find_missing_ids(df)
    df.to_csv(Path(out_file), index=False)
