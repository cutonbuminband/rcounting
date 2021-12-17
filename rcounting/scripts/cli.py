import argparse
from rcounting.scripts.validate import rule_dict


def main():
    parser = argparse.ArgumentParser(prog='rcounting')
    subparsers = parser.add_subparsers(dest='subparser')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--verbose', '-v', action='store_true',
                       help='Print more output')

    group.add_argument('--quiet', '-q', action='store_true',
                       help='Print less output')

    # Validate
    validate = subparsers.add_parser('validate',
                                     help=('Validate the reddit submission which '
                                           'contains the comment with id `comment_id` '
                                           'according to rule'))
    validate.add_argument('comment_id', help='The id of the comment to start logging from')
    validate.add_argument('--rule', choices=rule_dict.keys(), default='default',
                          help='Which rule to apply. Default is no double counting')

    td = subparsers.add_parser('update_directory',
                               help=('Update the thread directory located at'
                                     ' reddit.com/r/counting/wiki/directory'))
    td.add_argument('--pushshift', '-p', action='store_true',
                    help=('Use an online archive fetch older comments.'))

    td.add_argument('--dry-run', action='store_true',
                    help=('Write results to files instead of updating the wiki pages'))

    # Log thread
    log = subparsers.add_parser('log_thread',
                                help=('Log the reddit submission which'
                                      ' contains the comment with id `get_id`'))
    log.add_argument('--get_id', default='',
                     help=('The id of the leaf comment (get) to start logging from. '
                           'If no id is supplied, the script uses the get of '
                           'the last completed thread'))
    log.add_argument('-n', type=int, default=1,
                     help='The number of submissions to log. Default 1')
    log.add_argument('-o', '--output_directory', default='.',
                     help=('The directory to use for output. '
                           'Default is the current working directory'))

    log.add_argument('--sql', action='store_true',
                     help='Store output in a sql database instead of using csv files')

    log.add_argument('-a', '--all_counts', action='store_true',
                     help=('Log threads as far back in time as possible. '
                           'Warning: will take a while!'))

    args = parser.parse_args()
    if args.subparser == "validate":
        import rcounting.scripts.validate
        rcounting.scripts.validate.main(args)
    elif args.subparser == "update_directory":
        import rcounting.scripts.update_thread_directory
        rcounting.scripts.update_thread_directory.main(args)
    elif args.subparser == "log_thread":
        import rcounting.scripts.log_thread
        rcounting.scripts.log_thread.main(args)


if __name__ == "__main__":
    main()
