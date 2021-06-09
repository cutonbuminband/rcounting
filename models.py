from collections import defaultdict
from psaw import PushshiftAPI

api = PushshiftAPI()


class RedditPost():
    def __init__(self, post, cached=False):
        self.id = post.id
        self.timestamp = post.timestamp if hasattr(post, 'timestamp') else int(post.created_utc)
        self.username = post.username if hasattr(post, 'username') else str(post.author)
        self.body = post.body if hasattr(post, 'body') else post.selftext
        self.cached = cached or hasattr(post, 'cached')
        if hasattr(post, 'post_type'):
            self.post_type = post.post_type
        else:
            self.post_type = 'comment' if hasattr(post, 'body') else 'submission'
        self.search = api.search_comments if self.post_type == 'comment' else api.search_submissions
        if not self.cached:
            if (self.body == '[deleted]'
                or (self.body == '[removed]')
                or self.username == '[deleted]'):
                self.body, self.username = self.find_missing()
        self.cached = True

    def find_missing(self):
        post = next(self.search(ids=[self.id], metadata='true', limit=0))
        try:
            username = post.author
        except (KeyError, IndexError):
            username = self.username
        try:
            body = post.body if hasattr(post, 'body') else post.selftext
        except (KeyError, IndexError):
            body = self.body
        return body, username

    def to_dict(self):
        return {'username': self.username,
                'timestamp': self.timestamp,
                'comment_id': self.id,
                'thread_id': self.thread_id[3:],
                'body': self.body}


class Submission(RedditPost):
    def __init__(self, s):
        super().__init__(s)
        self.title = s.title
        self.thread_id = s.thread_id if hasattr(s, 'thread_id') else s.name

    def to_dict(self):
        return {'username': self.username,
                'timestamp': self.timestamp,
                'comment_id': self.id,
                'thread_id': self.thread_id[3:],
                'body': self.body,
                'title': self.title}

    def __repr__(self):
        return f'offline_submission(id={self.id})'


class Comment(RedditPost):
    def __init__(self, comment, tree=None):
        cached = (tree is not None)
        super().__init__(comment, cached)
        self.thread_id = comment.thread_id if hasattr(comment, 'thread_id') else comment.link_id
        self.parent_id = comment.parent_id
        self.is_root = (self.parent_id == self.thread_id)
        self.tree = tree

    def __repr__(self):
        return f'offline_comment(id={self.id})'

    def refresh(self):
        self.replies = self.tree.find_children(self)

    def parent(self):
        return self.tree.find_parent(self)


class CommentTree():
    def __init__(self, comments):
        self.comments = {x.id: x for x in comments}
        self.in_tree = {x.id: x.parent_id[3:] for x in comments}
        self.out_tree = edges_to_tree([(parent, child) for child, parent in self.in_tree.items()])

    def comment(self, comment_id):
        return Comment(self.comments[comment_id], self)

    def find_parent(self, comment):
        parent_id = self.in_tree[comment.id]
        return self.comment(parent_id)

    def find_children(self, comment):
        return [self.comment(x) for x in self.out_tree[comment.id]]


def edges_to_tree(edges):
    tree = defaultdict(list)
    for source, dest in edges:
        tree[source].append(dest)
    return tree
