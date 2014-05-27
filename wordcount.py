
def mapf(line):
    output = []
    for word in line.split():
        word = word.lower()
        if word.isalpha():
            output.append( (word, 1) )

    return output

def reducef(key, task, state):
    if state:
        return (key, task+int(state))
    else:
        return (key, task)
 