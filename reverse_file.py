# Python3 code to reverse the lines
# of a file using Stack.


# Creating Stack class (LIFO rule)
from datetime import datetime
import os


class Stack:

    def __init__(self):

        # Creating an empty stack
        self._arr = []

    # Creating push() method.
    def push(self, val):
        self._arr.append(val)

    def is_empty(self):

        # Returns True if empty
        return len(self._arr) == 0

    # Creating Pop method.
    def pop(self):

        if self.is_empty():
            print("Stack is empty")
            return

        return self._arr.pop()

# Creating a function which will reverse
# the lines of a file and Overwrites the
# given file with its contents line-by-line
# reversed


def reverse_file(filename):
    # Check if file already exists then avoid doing it again
    if (os.path.isfile('/home/newVol/data/r_' + filename)):
        return

    S = Stack()
    original = open("/home/newVol/data/" + filename)
    output = open("/home/newVol/data/r_" + filename, 'w')

    firstLine = True

    for line in original:
        S.push(line.rstrip("\n"))
        if(firstLine):
            output.write(S.pop()+"\n")
            firstLine = False

    original.close()

    while not S.is_empty():
        output.write(S.pop()+"\n")

    output.close()

