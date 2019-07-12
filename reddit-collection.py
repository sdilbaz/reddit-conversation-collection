# -*- coding: utf-8 -*-
"""
Created on Wed Jul 10 21:00:58 2019

@author: Serdarcan Dilbaz
"""

import praw
from praw.models import MoreComments
import pandas as pd
import datetime as dt
import json
from urllib.request import urlopen, build_opener
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import time
import os

def single_node(comment):
    sub_tree=ET.Element("response")
    try:
        if not comment.score_hidden:
            comment_score=comment.score
            ET.SubElement(sub_tree,"score").text=str(comment_score)
    except:
        pass
    try:
        commenter_username=comment.author.name
        ET.SubElement(sub_tree,"username").text=commenter_username
    except:
        pass
    try:
        comment_link=comment.permalink
        ET.SubElement(sub_tree,"permalink").text=comment_link
        
    except:
        pass
    try:
        comment_id=comment.id
        ET.SubElement(sub_tree,"response_id").text=comment_id
    except:
        pass
    try:
        comment_body=comment.body
        ET.SubElement(sub_tree,"body").text=comment_body
    except:
        pass
    return sub_tree


def generate_tree(comment):
    try:
        if comment.ups<3:
            return None
    except:
        return None
    comment=comment.refresh()
    if comment._replies is None:
        return single_node(comment)
    else:
        sub_tree=single_node(comment)        
        for reply in comment.replies:
            reply_tree=generate_tree(reddit.comment(reply))
            if not reply_tree is None:
                sub_tree.insert(-1,reply_tree)    
        return sub_tree

        
     
if not os.path.isfile("subreddit-list.txt"):
    subreddits=[]    
    opener = build_opener()
    
    main_url="http://redditlist.com/sfw"
    response=opener.open(main_url)
    
    page_html = response.read()
    soup=BeautifulSoup(page_html,'lxml')
    
    mydivs = soup.findAll("div", {"class": "listing-item"})
    for div in mydivs[int(len(mydivs)/3):-int(len(mydivs)/3)]:
        subreddits.append(div.attrs['data-target-subreddit'])
        
    for page_no in range(2,32):
#        print(page_no)
        
        main_url="http://redditlist.com/sfw?page="+str(page_no)
        response=opener.open(main_url)
        
        page_html = response.read()
        soup=BeautifulSoup(page_html,'lxml')
    
        mydivs = soup.findAll("div", {"class": "listing-item"})
        for div in mydivs[int(len(mydivs)/3):-int(len(mydivs)/3)]:
            subreddits.append(div.attrs['data-target-subreddit'])
    
    with open("subreddit-list.txt","w") as file:
        file.write('\n'.join(subreddits))

else:
    with open("subreddit-list.txt","r") as file:
        subreddits=file.read().split('\n')

with open(r"D:\Conversation_Code\reddit-credentials.json", "r") as file:
    creds = json.load(file)
client_id=creds['client_id'],
client_secret=creds['client_secret'],
user_agent=creds['user_agent'],
username=creds['username'],
password=creds['password']

reddit = praw.Reddit(client_id=creds['client_id'],
                     client_secret=creds['client_secret'],
                     user_agent=creds['user_agent'],
                     username=creds['username'],
                     password=creds['password'])

for subr in subreddits:
    subreddit = reddit.subreddit(subr)
    top_subreddit = subreddit.top(limit=None)
    for submission in top_subreddit:
        root = ET.Element("submission")
        try:
            author=submission.author
            poster_username=author.name
            ET.SubElement(root,"poster_username").text=poster_username
        except:
            pass
        try:
            post_link=submission.permalink
            ET.SubElement(root,"post_link").text=post_link
        except:
            pass
        try:
            post_subreddit=submission.subreddit.display_name
            ET.SubElement(root,"post_subreddit").text=post_subreddit
        except:
            pass
        
        try:
            if not submission.edited:
                post_time=time.ctime(submission.created)
            else:
                post_time=time.ctime(submission.edited)
            ET.SubElement(root,"post_time").text=post_time
        except:
            pass
        try:
            post_score=submission.score
            ET.SubElement(root,"post_score").text=str(post_score)
        except:
            pass
        try:
            post_title=submission.title
            ET.SubElement(root,"post_title").text=post_title
        except:
            pass
        try:
            post_upvote_ratio=submission.upvote_ratio
            ET.SubElement(root,"post_upvote_ratio").text=str(post_upvote_ratio)
        except:
            pass
        try:
            post_body=submission.selftext
            ET.SubElement(root,"post_body").text=post_body
        except:
            pass

        print('Adding more comments')
        submission.comments.replace_more(limit=256)#None)
        print('Added more comments')

        for comment in submission.comments:
            if comment.stickied or isinstance(comment, MoreComments):
                continue
            generated=generate_tree(comment)
            if not generated is None:
                root.insert(-1,generate_tree(comment))
            
        tree = ET.ElementTree(root)
        tree.write(submission.id+".xml")


