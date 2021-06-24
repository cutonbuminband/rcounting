from collections import defaultdict
deleted_phrases = ['[deleted]', '[removed]', '[banned]']


class RedditPost():
    def __init__(self, post, cached=False, api=None):
        self.id = post.id
        self.created_utc = post.created_utc
        self.author = str(post.author)
        self.body = post.body if hasattr(post, 'body') else post.selftext
        cached = cached or hasattr(post, 'cached')
        if hasattr(post, 'post_type'):
            self.post_type = post.post_type
        else:
            self.post_type = 'comment' if hasattr(post, 'body') else 'submission'
        if not cached and api is not None:
            if self.body in deleted_phrases or self.author in deleted_phrases:
                if self.post_type == 'comment':
                    search = api.search_comments
                elif self.post_type == 'submission':
                    search = api.search_submissions
                self.body, self.author = self.find_missing_content(search)
        self.cached = True

    def find_missing_content(self, search):
        try:
            post = next(search(ids=[self.id], metadata='true', limit=0))
        except StopIteration:
            return self.body, self.author
        author = post.author
        body = post.body if hasattr(post, 'body') else post.selftext
        return body, author

    def to_dict(self):
        return {'username': self.author,
                'timestamp': self.created_utc,
                'comment_id': self.id,
                'thread_id': self.thread_id[3:],
                'body': self.body}


class Submission(RedditPost):
    def __init__(self, s):
        super().__init__(s)
        self.title = s.title
        self.thread_id = s.thread_id if hasattr(s, 'thread_id') else s.name

    def to_dict(self):
        return {'username': self.author,
                'timestamp': self.created_utc,
                'comment_id': self.id,
                'thread_id': self.thread_id[3:],
                'body': self.body,
                'title': self.title}

    def __repr__(self):
        return f'offline_submission(id={self.id})'


class Comment(RedditPost):
    def __init__(self, comment, tree=None):
        super().__init__(comment)
        self.thread_id = comment.thread_id if hasattr(comment, 'thread_id') else comment.link_id
        self.parent_id = comment.parent_id
        self.tree = tree

    def __repr__(self):
        return f'offline_comment(id={self.id})'

    @property
    def is_root(self):
        return (self.parent_id == self.thread_id)

    @property
    def replies(self):
        return self.tree.find_children(self)

    def refresh(self):
        pass

    def parent(self):
        return self.tree.find_parent(self)


class CommentTree():
    def __init__(self, comments, reddit=None):
        self.comments = {x.id: x for x in comments}
        self.in_tree = {x.id: x.parent_id[3:] for x in comments if not self.is_root(x)}
        self.out_tree = edges_to_tree([(parent, child) for child, parent in self.in_tree.items()])
        self.reddit = reddit

    def __len__(self):
        return len(self.in_tree.keys())

    def is_root(self, comment):
        thread_id = comment.thread_id if hasattr(comment, 'thread_id') else comment.link_id
        return comment.parent_id == thread_id

    def add_missing_comments(self, comment_id):
        comments = []
        praw_comment = self.reddit.comment(comment_id)
        try:
            praw_comment.refresh()
        except Exception as e:
            print(e)
            pass
        for i in range(9):
            comments.append(praw_comment)
            if praw_comment.is_root:
                break
            praw_comment = praw_comment.parent()
        for comment in comments:
            if comment.id not in self.comments:
                child_id = comment.id
                parent_id = comment.parent_id
                self.comments.update({child_id: comment})
                if not comment.is_root:
                    self.in_tree[child_id] = parent_id[3:]
                    self.out_tree[parent_id[3:]].append(child_id)

    def comment(self, comment_id):
        try:
            return Comment(self.comments[comment_id], self)
        except KeyError:
            if self.reddit is not None:
                self.add_missing_comments(comment_id)
                return Comment(self.comments[comment_id], self)
            else:
                raise

    def find_parent(self, comment):
        if comment.is_root:
            raise StopIteration
        parent_id = self.in_tree[comment.id]
        return self.comment(parent_id)

    def find_children(self, comment):
        return [self.comment(x) for x in self.out_tree[comment.id]]


def edges_to_tree(edges):
    tree = defaultdict(list)
    for source, dest in edges:
        tree[source].append(dest)
    return tree


def comment_to_dict(comment):
    try:
        return comment.to_dict()
    except AttributeError:
        return Comment(comment).to_dict()
