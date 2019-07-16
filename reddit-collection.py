# -*- coding: utf-8 -*-
"""
Created on Wed Jul 10 21:00:58 2019

@author: Serdarcan Dilbaz
"""

import praw
from praw.models import MoreComments
import json
from urllib.request import build_opener
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import time
import os
import glob
from multiprocessing import Process, Manager
import argparse
from argparse import RawTextHelpFormatter


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


def generate_tree(comment,depth_limit):
    try:
        if comment.ups<3:
            return None
        if comment.depth>depth_limit:
            return None
    except:
        return None
    comment=comment.refresh()
    if comment._replies is None:
        return single_node(comment)
    else:
        sub_tree=single_node(comment)        
        for reply in comment.replies:
            reply_tree=generate_tree(reply,depth_limit)
            if not reply_tree is None:
                sub_tree.insert(-1,reply_tree)    
        return sub_tree

def retrieve_subreddit_list():
    if os.path.isfile("subreddit-list.txt"):
        with open("subreddit-list.txt","r") as file:
            subreddits=file.read().split('\n')
    else:
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
            main_url="http://redditlist.com/sfw?page="+str(page_no)
            response=opener.open(main_url)
            
            page_html = response.read()
            soup=BeautifulSoup(page_html,'lxml')
        
            mydivs = soup.findAll("div", {"class": "listing-item"})
            for div in mydivs[int(len(mydivs)/3):-int(len(mydivs)/3)]:
                subreddits.append(div.attrs['data-target-subreddit'])
        
        with open("subreddit-list.txt","w") as file:
            file.write('\n'.join(subreddits))
    
    return subreddits

def subm_from_subreddit(subreddit_list,sub_list_dir,json_loc):
    while len(subreddit_list):
        subreddit=subreddit_list.pop()
        list_loc=os.path.join(sub_list_dir,subreddit+".txt")
        if not os.path.isfile(list_loc):
            with open(json_loc, "r") as file:
                creds = json.load(file)
            
            reddit = praw.Reddit(client_id=creds['client_id'],
                             client_secret=creds['client_secret'],
                             user_agent=creds['user_agent'],
                             username=creds['username'],
                             password=creds['password'])
            
            subreddit = reddit.subreddit(subreddit)
            top_subreddit = subreddit.top(limit=None)
            with open(list_loc,"w") as file:
                for submission in top_subreddit:
                    file.write(submission.id+"\n")
        
def save_convs(subreddit_list,json_loc,saving_dir,depth_limit):
    while len(subreddit_list):
        subreddit=subreddit_list.pop()
        subreddit_conv_dir=os.path.join(saving_dir,"conversations",subreddit)
        if not os.path.isdir(subreddit_conv_dir):
            os.mkdir(subreddit_conv_dir, exist_ok=True)
        
        file=open(os.path.join(saving_dir,"submissions"),"r")
        subm_id=file.readline().strip()
        while subm_id:
            if not os.path.isfile(os.path.join(saving_dir,"conversations",subm_id+".xml")):
                with open(json_loc, "r") as file:
                    creds = json.load(file)
            
                reddit = praw.Reddit(client_id=creds['client_id'],
                                 client_secret=creds['client_secret'],
                                 user_agent=creds['user_agent'],
                                 username=creds['username'],
                                 password=creds['password'])
                
                submission=reddit.submission(id=subm_id)
                
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
            
                submission.comments.replace_more(limit=256)
            
                for comment in submission.comments:
                    if comment.stickied or isinstance(comment, MoreComments):
                        continue
                    generated=generate_tree(comment,depth_limit)
                    if not generated is None:
                        root.insert(-1,generated)
                    
                tree = ET.ElementTree(root)
                tree.write(os.path.join(saving_dir,"conversations",subm_id+".xml"))
                
                subm_id=file.readline().strip()
            file.close()
            
        
def valid_dir(argument):
    directory=str(argument)
    if not os.path.isdir(directory):
        msg="Choose a valid directory. You entered: %s" %directory
        raise argparse.ArgumentTypeError(msg)
    return directory

def positive_int(argument):
    num=int(argument)
    if num<1:
        msg="Parameter must be a positive number. You entered: %s" %argument
        raise argparse.ArgumentTypeError(msg)
    return num



if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Scrapes Reddit comments as conversations', formatter_class=RawTextHelpFormatter)
    parser.add_argument('credentials_dir',help='directory for reddit credentials', type=valid_dir)
    parser.add_argument('saving_dir',help='directory for saving collected data', type=valid_dir)
    parser.add_argument('num_workers',help='number of reddit accounts to use for scraping', type=positive_int)
    parser.add_argument('depth_limit',help='depth limit for collecting conversations', type=positive_int)

    args = parser.parse_args()
    if args.num_workers<1:
        raise argparse.ArgumentTypeError("Number of workers must be a positive number.")
        
    possible_workers=len(glob.glob(os.path.join(args.credentials_dir,"*.json")))
    if not possible_workers:
        raise argparse.ArgumentTypeError("No json credentials in the credentials directory")
    
    num_workers=min(args.num_workers, possible_workers)
    
    manager=Manager()
    
    sub_list_dir=os.path.join(args.saving_dir,"submissions")
    os.makedirs(sub_list_dir, exist_ok=True)
    subreddits=retrieve_subreddit_list()
    subreddit_list=manager.list(subreddits[::-1])
    cred_jsons=glob.glob(os.path.join(args.credentials_dir,"*.json"))[:num_workers]

    # Submission listing for each subreddit started
    
    processes=[]
    for json_loc in cred_jsons:
        p=Process(target=subm_from_subreddit,args=[subreddit_list,sub_list_dir,json_loc])
        processes.append(p)
        p.start()
        
    while any([p.is_alive() for p in processes]):
        time.sleep(60)

    [p.join() for p in processes]
    
    # Submission Listing Complete
        
    conv_dir=os.path.join(args.saving_dir,"conversations")
    os.makedirs(conv_dir, exist_ok=True)
    subreddits=retrieve_subreddit_list()
    subreddit_list=manager.list(subreddits[::-1])
    
    for json_loc in cred_jsons:
        p=Process(target=save_convs,args=[subreddit_list,json_loc,args.saving_dir,args.depth_limit])
        processes.append(p)
        p.start()
        
    while any([p.is_alive() for p in processes]):
        time.sleep(60)

    [p.join() for p in processes]

