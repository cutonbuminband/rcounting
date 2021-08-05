from collections import defaultdict, deque
from utils import deleted_phrases


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
        RedditPost.__init__(self, comment)
        self.thread_id = comment.thread_id if hasattr(comment, 'thread_id') else comment.link_id
        self.parent_id = comment.parent_id
        self.is_root = (self.parent_id == self.thread_id)
        self.tree = tree

    def __repr__(self):
        return f'offline_comment(id={self.id})'

    def refresh(self):
        pass

    def walk_up_tree(self, limit=None):
        return self.tree.walk_up_tree(self, limit)

    def parent(self):
        return self.tree.parent(self)

    @property
    def replies(self):
        return self.tree.find_children(self)

    @property
    def get_missing_replies(self):
        return self.tree.get_missing_replies

    @property
    def depth(self):
        return self.tree.find_depth(self)


class Tree():
    def __init__(self, nodes, tree):
        self.tree = tree
        self.nodes = nodes
        self.depths = {}

    @property
    def reversed_tree(self):
        return edges_to_tree([(parent, child) for child, parent in self.tree.items()])

    def parent(self, node):
        parent_id = self.tree[node.id]
        return self.node(parent_id)

    def find_children(self, node):
        return [self.node(x) for x in self.reversed_tree[node.id]]

    def walk_up_tree(self, node, limit=None):
        if isinstance(node, str):
            try:
                node = self.node(node)
            except KeyError:
                return None
        if node.id not in self.tree and node.id not in self.nodes:
            return None
        nodes = [node]
        counter = 1
        while node.id in self.tree and not getattr(node, 'is_root', False):
            if limit is not None and counter >= limit:
                break
            node = self.parent(node)
            nodes.append(node)
            counter += 1
        return nodes

    def walk_down_tree(self, node, limit=None):
        if node.id not in self.nodes and node.id not in self.reversed_tree:
            return None
        result = [node]
        while node.id in self.reversed_tree:
            node = self.find_children(node)[0]
            result.append(node)
        return result

    def __len__(self):
        return len(self.tree.keys())

    def node(self, node_id):
        return self.nodes[node_id]

    def delete_node(self, node):
        del self.nodes[node.id]
        if node.id in self.tree:
            del self.tree[node.id]

    def delete_subtree(self, node):
        queue = deque([node])
        while queue:
            node = queue.popleft()
            queue.extend(self.find_children(node))
            self.delete_node(node)

    def find_depth(self, node):
        if node.id in self.root_ids:
            return 0
        elif node.id in self.depths:
            return self.depths[node.id]
        else:
            depth = 1 + self.find_depth(node.parent())
            self.depths[node.id] = depth
            return depth

    @property
    def deepest_node(self):
        max_depth = 0
        result = None
        for leaf in self.leaves:
            if depth := self.find_depth(leaf) > max_depth:
                max_depth = depth
                result = leaf
        return result

    @property
    def leaves(self):
        leaf_ids = set(self.nodes.keys()) - set(self.tree.values())
        return [self.node(leaf_id) for leaf_id in leaf_ids]

    @property
    def roots(self):
        return [self.node(root_id) for root_id in self.root_ids]

    @property
    def root_ids(self):
        root_ids = (set(self.nodes.keys()) | set(self.tree.values())) - set(self.tree.keys())
        root_ids = [[x] if x in self.nodes else self.reversed_tree[x] for x in root_ids]
        return [root_id for ids in root_ids for root_id in ids]

    def add_nodes(self, new_nodes, new_tree):
        self.tree.update(new_tree)
        self.nodes.update(new_nodes)


class CommentTree(Tree):
    def __init__(self, comments, reddit=None, get_missing_replies=True, verbose=True):
        tree = {x.id: x.parent_id[3:] for x in comments if not is_root(x)}
        comments = {x.id: x for x in comments}
        super().__init__(comments, tree)
        self.reddit = reddit
        self.get_missing_replies = get_missing_replies
        self.verbose = verbose
        self.comment = self.node

    def node(self, comment_id):
        if comment_id not in self.tree and self.reddit is not None:
            self.add_missing_parents(comment_id)
        return Comment(super().node(comment_id), self)

    def add_nodes(self, comments):
        new_comments = {x.id: x for x in comments}
        new_tree = {x.id: x.parent_id[3:] for x in comments if not is_root(x)}
        super().add_nodes(new_comments, new_tree)

    @property
    def comments(self):
        return self.nodes.values()

    def parent(self, node):
        parent_id = self.tree[node.id]
        return self.node(parent_id)

    def add_missing_parents(self, comment_id):
        comments = []
        praw_comment = self.reddit.comment(comment_id)
        try:
            praw_comment.refresh()
            if self.verbose:
                print(f"Fetching ancestors of comment {praw_comment.id}")
        except Exception as e:
            print(e)
            pass
        for i in range(9):
            comments.append(praw_comment)
            if praw_comment.is_root:
                break
            praw_comment = praw_comment.parent()
        self.add_nodes(comments)

    def find_children(self, comment):
        children = [self.comment(x) for x in self.reversed_tree[comment.id]]
        if not children and self.get_missing_replies:
            if self.verbose:
                print(f"Fetching replies to comment {comment.id}")
            children = self.add_missing_replies(comment)
        by_date = sorted(children, key=lambda x: x.created_utc)
        return sorted(by_date, key=lambda x: x.body in deleted_phrases)

    def add_missing_replies(self, comment):
        if comment.id not in self.nodes:
            self.add_nodes([comment])
        praw_comment = self.reddit.comment(comment.id)

        praw_comment.refresh()
        replies = praw_comment.replies
        replies.replace_more(limit=None)
        replies = replies.list()
        if replies:
            self.add_nodes(replies)
            return [self.comment(x.id) for x in replies]
        else:
            return []

    def is_broken(self, comment):
        if comment.is_root:
            return False
        parent = comment.parent()
        replies = self.add_missing_replies(parent)
        if comment.id not in [x.id for x in replies]:
            return True
        return False

    def prune(self, side_thread):
        nodes = self.roots
        queue = deque([(node, side_thread.get_history(node)) for node in nodes])
        while queue:
            node, history = queue.popleft()
            is_valid, new_history = side_thread.is_valid_count(node, history)
            if is_valid:
                queue.extend([(x, new_history) for x in self.find_children(node)])
            else:
                self.delete_subtree(node)


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


def is_root(comment):
    try:
        return comment.is_root
    except AttributeError:
        thread_id = comment.thread_id if hasattr(comment, 'thread_id') else comment.link_id
        return comment.parent_id == thread_id
