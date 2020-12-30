# Author: Darren Colby
# Date: 12/29/2020
# Purpose: To efficiently search for a string in a list of strings

class TrieNode:
    def __init__(self, name, next_node):
        self.name = name
        self.next_node = next_node


class Trie:
    def __init__(self, word_list):
        tree_dict = dict()

        for word in word_list:
            for letter in word:
                root = TrieNode(letter, word[1])

                if root.name not in tree_dict.keys():
                    tree_dict[root.name] = root.next_node
