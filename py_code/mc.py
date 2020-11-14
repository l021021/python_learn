import numpy as np


class Mc:
    # """   3
    # """   2
    # """ 5 1 6
    # """   4
    # """
    # """ sides numbering
    # """1  2  3
    # """2
    # """3
    # """ row and column numering
    def __init__(self):
        self.sides = np.full((6, 3, 3), 0)
        for i in range(6):
            self.sides[i] = np.full((3, 3), i + 1)

    def tilt(self, side, time=1):
        # clockwise
        if time == 2:
            self.tilt(side, 1)
            self.tilt(side, 1)
        elif time == 3:
            self.tilt(side, 1)
            self.tilt(side, 1)
            self.tilt(side, 1)

        else:
            if side == 'left':
                temp = self.sides[0, :, 0].copy()
                self.sides[0, :, 0] = self.sides[3, :, 0]
                self.sides[3, :, 0] = self.sides[2, :, 0]
                self.sides[2, :, 0] = self.sides[1, :, 0]
                self.sides[1, :, 0] = temp
                self.sides[4] = rotate(self.sides[4])
            elif side == 'right':
                temp = self.sides[0, :, 2].copy()
                self.sides[0, :, 2] = self.sides[3, :, 2]
                self.sides[3, :, 2] = self.sides[2, :, 2]
                self.sides[2, :, 2] = self.sides[1, :, 2]
                self.sides[1, :, 2] = temp
                self.sides[5] = rotate(self.sides[5])
            elif side == 'middle':
                for i in range(3):
                    self.tilt('right', 1)
                    self.tilt('left', 1)
            else:
                pass

    def pan(self, side, time=1):
        # clockwise
        if time == 2:
            self.pan(side, 1)
            self.pan(side, 1)
        elif time == 3:
            self.pan(side, 1)
            self.pan(side, 1)
            self.pan(side, 1)

        else:
            if side == 'upper':
                temp = self.sides[0, 0, :, ].copy()
                self.sides[0, 0, :] = self.sides[4, 0, :]
                self.sides[4, 0, :] = self.sides[2, 0, :]
                self.sides[2, 0, :] = self.sides[5, 0, :]
                self.sides[5, 0, :] = temp
                self.sides[1] = rotate(self.sides[1])
            elif side == 'bottom':
                temp = self.sides[0, 2, :].copy()
                self.sides[0, 2, :] = self.sides[4, 2, :]
                self.sides[4, 2, :] = self.sides[2, 2, :]
                self.sides[2, 2, :] = self.sides[5, 2, :]
                self.sides[5, 2, :] = temp
                self.sides[3] = rotate(self.sides[3])
            elif side == 'middle':
                for i in range(3):
                    self.pan('upper', 1)
                    self.pan('bottom', 1)
            else:
                pass


# def pan(self, side, time=1):
#     # clockwise
#     if time == 2:
#         self.pan(side, 1)
#         self.pan(side, 1)
#     elif time == 3:
#         self.pan(side, 1)
#         self.pan(side, 1)
#         self.pan(side, 1)

#     else:
#         if side == 'upper':
#             temp = self.sides[0, 0, :, ].copy()
#             self.sides[0, 0, :] = self.sides[4, 0, :]
#             self.sides[4, 0, :] = self.sides[2, 0, :]
#             self.sides[2, 0, :] = self.sides[5, 0, :]
#             self.sides[5, 0, :] = temp
#             self.sides[1, ...].transpose()
#         elif side == 'bottom':
#             temp = self.sides[0, 2, :].copy()
#             self.sides[0, 2, :] = self.sides[4, 2, :]
#             self.sides[4, 2, :] = self.sides[2, 2, :]
#             self.sides[2, 2, :] = self.sides[5, 2, :]
#             self.sides[5, 2, :] = temp
#             self.sides[3, ...] = rotate(self.sides[3, ...])  #todo
#         elif side == 'middle':
#             for i in range(3):
#                 self.pan('upper', 1)
#                 self.pan('bottom', 1)
#         else:
#             pass


def rotate(array):  # input is 3x3 array
    # li1 = array[0]
    # li2 = array[1]
    # li3 = array[2]
    # array[:, 2] = li1
    # array[:, 1] = li2
    # array[:, 0] = li3
    arraycopy = array.copy()
    array[:, 2], array[:, 1], array[:, 0] = np.vsplit(arraycopy, 3)
    return array


mc = Mc()
# print(mc.sides)

# mc.tilt('right', 1)
# mc.tilt('left', 2)
mc.tilt('middle', 3)
# # mc.pan('upper', 1)
mc.pan('bottom', 1)
# mc.pan('middle', 3)

# print(mc.sides)
mc.tilt('right', 1)
# print(mc.sides)
mc.tilt('right', 3)
# mc.tilt('right', 1)
mc.pan('bottom', 3)
mc.tilt('middle', 1)

# mc.tilt('right', 3)
print(mc.sides)
