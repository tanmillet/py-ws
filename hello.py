#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# print("Hello Python!", "This is Test!")
# print(1024 * 768)
#
# print(r'''hello,\n
# world''')
#
# print(ord("A"))

# str = '中文'
# print(str)
# print(len(str))
# """
#
# """

# s1 = 721
# s2 = 853
# if s1 >= 18:
#     print("the value is", s1)
# elif s2 > s1:
#     print('the value is', s1 + s2)
# else:
#     print('the value is', s2)
# x = 1
# if x:
#     print('True')

# birth = int(input('birth:'))
# if birth < 2000:
#     print('the birth value:', birth)
# elif birth > 3000:
#     print('the birth value:', birth)
# else:
#     print('the birth value is error')


# names = ['A', 'B', 'C', 'D']
# for name in names:
#     print(name)


# r = s1 / s2
# print('{1:.1f}%'.format('小明',r))
# print('rest:%f' % r)
#
# class_mates = ['B','G']
# class_mates.insert(0,'A')
# class_mates.pop(1)
# print(len(class_mates))
# print(class_mates)
# print(class_mates[0])
#
# tiple_items = ('a' , class_mates)
# print(tiple_items[1])


dict_items = {'terry': 100,'chris': 90}

if  'terry' in dict_items:
    print(dict_items['terry'])

if dict_items.get('chris'):
    print(dict_items['chris'])