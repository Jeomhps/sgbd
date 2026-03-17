class Tuple:

    def __init__(self, size):
        self.size = size
        self.val = [0] * size

    def __str__(self):
        return "\t".join(map(str, self.val))+"\t"