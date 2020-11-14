from tkinter import *


def msg():
    print('stdout')


top = Frame()
top.pack()
Label(top, text='Hellp world').pack(side=TOP)
widget = Button(top, text='press', command=msg)
widget.pack(side=BOTTOM)
top.mainloop()
