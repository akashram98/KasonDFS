# #Import required module
# from colorama import Fore, Back, Style
#
# #Print text using font color
# # print(Fore.CYAN + 'Welcome to Linuxhint')
# #Print text using font color and BRIGHT style
# def println(text):
#     print(Fore.RED + Style.BRIGHT + text, end='')
#     a=[[],[]]
#
# println("abcd hghgh")


if __name__=='__main__':
    q="akash_ram_praveen_raj"
    res = ""
    q = q.split("_")
    res = res+q.pop(0)
    for q1 in q:
        res = res+q1.title()
    print(res)
    