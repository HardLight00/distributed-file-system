class DirectoryTree:
    def __init__(self):
        self.root = Node(parent=None, path='/')
        self.current = self.root

    def is_exist(self, directory):
        return self.root.get_child_index(directory) != -1

    def put(self, path):
        for child in self.current.children:
            if path == child.path:
                break
        else:
            node = Node(parent=self.current, path=path)
            self.current.add_children(node)
            return node.path

    def remove(self, path):
        for child in self.current.children:
            if path == child.path:
                self.current.children.remove(child)

    def go_to_parent(self):
        if not self.is_root():
            self.current = self.current.parent

    def go_to_child(self, path):
        ind = self.current.get_child_index(path)
        if ind != -1:
            self.current = self.current.children[ind]
        else:
            raise FileNotFoundError(f'Error: current directory does not contain {path} as a child path')

    def get_current(self):
        return self.current

    def is_root(self):
        return self.current == self.root


class Node:
    def __init__(self, parent, path, children=None):
        if children is None:
            children = []
        self.parent = parent
        self.path = path if parent is None else f'/{path}' if parent.path == '/' else f'{parent.path}/{path}'
        self.children = children

    def get_child_index(self, path):
        for index, child in enumerate(self.children):
            if child.path.split('/')[-1] == path:
                ind = index
                break
        else:
            ind = -1
        return ind

    def add_children(self, node):
        self.children.append(node)

    def remove_children(self, path):
        ind = self.get_child_index(path)
        if ind != -1:
            self.children.remove(self.children[ind])
