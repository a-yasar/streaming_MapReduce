#!/usr/bin/env/python

def mapf(line):
    """
        User defined map function
    """
    output = []
    for word in line.split():
        word = word.lower()
        if word.isalpha():
            output.append( (word, 1) )

    return output

def reducef(key, task, state):
    """
        User defined reduce function
    """
    if state:
        return (key, task+int(state))
    else:
        return (key, task)
 