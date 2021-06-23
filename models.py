from collections import defaultdict
deleted_phrases = ['[deleted]', '[removed]', '[banned]']


class RedditPost():
    def __init__(self, post, cached=False, api=None):
        self.id = post.id
        self.created_utc = post.created_utc
        self.author = str(post.author)
        self.body = post.body if hasattr(post, 'body') else post.selftext
        self.cached = cached or hasattr(post, 'cached')
        if hasattr(post, 'post_type'):
            self.post_type = post.post_type
        else:
            self.post_type = 'comment' if hasattr(post, 'body') else 'submission'
        if not self.cached and api is not None:
            if (self.body in deleted_phrases or self.author in deleted_phrases):
                if self.post_type == 'comment':
                    search = api.search_comments
                elif self.post_type == 'submission':
                    search = api.search_submissions
                self.body, self.author = self.find_missing(search)
        self.cached = True

    def find_missing(self, search):
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
    def __init__(self, comment, tree=None, psaw=None):
        cached = (tree is not None)
        super().__init__(comment, cached, psaw)
        self.thread_id = comment.thread_id if hasattr(comment, 'thread_id') else comment.link_id
        self.parent_id = comment.parent_id
        self.tree = tree

    def __repr__(self):
        return f'offline_comment(id={self.id})'

    @property
    def is_root(self):
        if self.tree is None:
            return (self.parent_id == self.thread_id)
        else:
            return self.tree.is_root(self)

    @property
    def replies(self):
        return self.tree.find_children(self)

    def refresh(self):
        pass

    def parent(self):
        return self.tree.find_parent(self)


class CommentTree():
    def __init__(self, comments, root_comment=None):
        self.root_comment = root_comment
        if root_comment is not None:
            comments = [x for x in comments if int(x.id, 36) >= int(root_comment.id, 36)]
        self.comments = {x.id: x for x in comments}
        self.in_tree = {x.id: x.parent_id[3:] for x in comments if not self.is_root(x)}
        self.out_tree = edges_to_tree([(parent, child) for child, parent in self.in_tree.items()])

    def __len__(self):
        return len(self.in_tree.keys())

    def is_root(self, comment):
        if self.root_comment is not None:
            return comment.id <= self.root_comment.id
        else:
            thread_id = comment.thread_id if hasattr(comment, 'thread_id') else comment.link_id
            return comment.parent_id == thread_id

    def add_comment(self, comment):
        if comment.id not in self.comments:
            child = comment.id
            parent = comment.parent_id
            self.comments.update({child: comment})
            if not self.is_root(comment):
                self.in_tree[child] = parent[3:]
                self.out_tree[parent[3:]].append(child)
        return self.comment(comment.id)

    def comment(self, comment_id):
        return Comment(self.comments[comment_id], self)

    def find_parent(self, comment):
        if comment.is_root:
            raise StopIteration
        parent_id = self.in_tree[comment.id]
        return self.comment(parent_id)

    def find_children(self, comment):
        return [self.comment(x) for x in self.out_tree[comment.id]]

    def missing_comments(self):
        children = set(self.comments.keys())
        parents = set(self.in_tree.values())
        return parents - children


def edges_to_tree(edges):
    tree = defaultdict(list)
    for source, dest in edges:
        tree[source].append(dest)
    return tree


def comment_to_dict(comment):
    try:
        result = comment.to_dict()
    except AttributeError:
        result = {'comment_id': comment.id,
                  'username': str(comment.author),
                  'timestamp': comment.created_utc}
    keys = ['comment_id', 'username', 'timestamp']
    return {k: v for k, v in result if k in keys}
