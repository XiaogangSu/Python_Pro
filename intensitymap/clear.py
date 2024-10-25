#-*- coding: utf-8 -*-
import tkinter as tk
from tkinter import filedialog

# 创建一个Tkinter窗口对象
root = tk.Tk()
root.withdraw()  # 隐藏主窗口

# 打开文件选择对话框
file_path = filedialog.askopenfilename()

# 打印选择的文件路径
print("选择的文件路径：", file_path)
