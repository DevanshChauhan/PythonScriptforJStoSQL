import json
import sqlite3
import argparse
from pathlib import Path



def load_json_from_js(p):
    """Takes a path to Twitter ad impression data and returns parsed JSON.
    
    Note that the Twitter files are *not* valid JSON but a Javascript file
    with a blog of JSON assigned to a Javascript variable, so some 
    preprocessing is needed.""" 
    return json.loads(p.read_text()[33:])


# Don't touch this function!
def populate_db(adsjson, db):
    """Takes a blob of Twitter ad impression data and pushes it into our database.
    
    Note that this is responsible for avoiding redundant entries. Furthermore,
    it should be robust to errors and always get as much data in as it can.
    """ 
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    try:
        json2db(adsjson, cur)
    except Exception as e:
        # We'd prefer no exceptions reached this level.
        print(f"Error occurred: {e}")
        print("There was a problem with the loader. This shouldn't happen")
    conn.commit()
    conn.close()


def json2db(adsjson, cur):
    """Processes the JSON and INSERTs it into the db via the cursor, cur"""
    sql_insert_deviceInfo = '''INSERT OR IGNORE INTO deviceInfo (osType, deviceId, deviceType) VALUES (?, ?, ?)'''
    promoted_tweet_info_sql = '''INSERT OR IGNORE INTO "promotedTweetInfo" (tweetId, tweetText, urls, MediaUrls) VALUES (?, ?, ?, ?)'''
    advertiser_info_sql = '''INSERT OR IGNORE INTO "advertiserInfo" (advertiserName, screenName) VALUES (?, ?)'''
    targeting_criteria_sql = ''' INSERT OR IGNORE INTO "TargetingCriteria" (id, targetingType , targetingValue) VALUES (? ,?, ?)'''
    impressions_sql = ''' INSERT  INTO "impressions" (id, deviceInfo , displayLocation, promotedTweetInfo, impressionTime, advertiserInfo) VALUES (?, ?, ?, ?, ?,?)'''
    check_redundancy_val = '''SELECT 1 FROM TargetingCriteria  WHERE  targetingType =?  AND targetingValue = ? '''
    check_redundancy_val_1 = '''SELECT * FROM TargetingCriteria  WHERE  targetingType =?  AND targetingValue IS NULL '''
    check_redundancy_val_3 = '''SELECT * FROM impressions  WHERE   deviceInfo =? AND displayLocation =?  AND promotedTweetInfo = ? AND impressionTime= ? AND  advertiserInfo=?'''
    
 
    matched_targeting_criteria_sql = '''INSERT INTO "matchedTargetingCriteria" ("impression", "criteria") VALUES (?, ?)'''

#To keep an account for id's we need to have counter variables
    counter_main = 0
    counter_main2 = 0
    for ad in adsjson:
        counter_main = counter_main+1
        counter_main2= counter_main2+1

        try:
            impressions = ad['ad']['adsUserData']['adImpressions']['impressions']
            for impression in impressions:
                device_info = impression.get('deviceInfo', {})
                device_id = device_info.get('deviceId')
                if device_id:
                    cur.execute(sql_insert_deviceInfo, (device_info.get('osType'), device_id, device_info.get('deviceType')))
#Insertion into Device_Info_SQL      

                promoted_tweet_info = impression.get('promotedTweetInfo', {})
                tweet_id = promoted_tweet_info.get('tweetId')
                promoted_tweet_url = promoted_tweet_info.get('urls')
                promoted_tweet_mediaUrls = promoted_tweet_info.get('mediaUrls')

# if condtitions for ensuring Null value and  primary key constraints 
                if promoted_tweet_url:
                    url_insert1 = json.dumps(promoted_tweet_url)
                else:
                    url_insert1 = None
                    
                if promoted_tweet_mediaUrls:
                    url_media_insert1 = json.dumps(promoted_tweet_url)
                else:
                    url_media_insert1 = None
#Promoted tweet Info Table
                if tweet_id:
                    cur.execute(promoted_tweet_info_sql, (tweet_id, promoted_tweet_info.get('tweetText'), url_insert1 , url_media_insert1, ))

#Advertisor Info Table
                advertiser_info = impression.get('advertiserInfo', {})
                advertiser_name = advertiser_info.get('advertiserName')
                advertiser_screenname = advertiser_info.get('screenName')
                if advertiser_name:
                    cur.execute(advertiser_info_sql, (advertiser_name ,advertiser_screenname, ))
                
                mathcedtargertinfo = impression.get('matchedTargetingCriteria' , [])


#insertion into impressions_Table
                device_id2 = device_info.get('deviceId')
                disp_location_info =  impression.get('displayLocation')
                imp_time = impression.get('impressionTime')
                cur.execute(check_redundancy_val_3, (device_id2, disp_location_info, tweet_id,imp_time, advertiser_name ))
                resultx = cur.fetchone()
# check_redundancy_val_3, Helps removing redundancy 
                if resultx is  None:
                    cur.execute(impressions_sql, (counter_main2 , device_id2, disp_location_info, tweet_id, imp_time, advertiser_name ))
                    temp1 = counter_main2
                    counter_main2 = counter_main2+1
                
# here we also remove the redundancy 
                for x1 in mathcedtargertinfo:     
                    target_type =  x1.get('targetingType') 
                    target_value =  x1.get('targetingValue')
                    #matchedTargetingCriteria"
                    cur.execute(matched_targeting_criteria_sql,(temp1,counter_main))
                    if target_value == None and target_type!=None:
                        cur.execute(check_redundancy_val_1, (target_type,))
                        result2 = cur.fetchone()
                        if result2 is None:
                            cur.execute(targeting_criteria_sql, (None ,target_type, None ))
                            counter_main = counter_main+1
                    else:
                        cur.execute(check_redundancy_val, (target_type, target_value))
                        result = cur.fetchone()
                        if result is  None:
                            cur.execute(targeting_criteria_sql, (None ,target_type, target_value ))
                            counter_main = counter_main+1

        except Exception as e:
            print(f"The Error is : {e}")
    
    # THIS IS WHAT YOU SHOULD MODIFY!
    # Feel free to add helper functions...you don't *need* to make a giant
    # hard to test function...indeed, that will come up in code review!
    pass


# DO NOT MODIFY ANYTHING BELOW!
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Load JSON from Twitter's ad-impressions.js into our database.")
    parser.add_argument('--source',  
                        type=Path,
                        default=Path('./ad-impressions.js'),
                        help='path to source  file')    
    parser.add_argument('--output', 
                        type=Path,
                        default=Path('./twitterads.db'),
                        help='path to output DB')    
    args = parser.parse_args()
    
    print('Loading JSON.')
    ads_json = load_json_from_js(args.source)
    print('Populating database.')    
    populate_db(ads_json, args.output)
    print('Done')