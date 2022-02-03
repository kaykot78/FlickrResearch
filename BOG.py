import pandas as pd





class BOG:
    
    '''Output is for mapping individual tags
    get the owner, and latitude/longitude coordinates for each instance of a tag. Takes in a csv that 
    contains output from tags_to_BOG. Puts out a dataframe.'''
    def get_tag_info(df, tag):
        
        tagsList = [] # list of tags
        tag_tuple = ()   #tuples
        tag_tuple_list = []   #list of tuples
        
        
        
        #itterate over the dataframe to get info
        for row in df.itertuples(index=False):
            #These positions can be adjusted for a different column configuration
            owner = str(row[5])
            #owner2 = owner + ' '
            tags = str(row[8]).lower()
            locCode = row[10]
            lat = str(row[3])
            lon = str(row[4])
            #coord = lat + ' ' + lon + ' '
            #store the tags individualiy in a list
            tagsList = tags.split()
            
            
            #go through the tag list, pull out the info for the specific tag
            for tags in tagsList:
                if tags == tag:
                    tag_tuple = (tags, owner, lat,lon, locCode)
                    tag_tuple_list.append(tag_tuple)
                    
                
        tagsDf = pd.DataFrame(tag_tuple_list,columns = ['tag', 'owner', ' latitude', ' longitude', 'LocCode'])
        return tagsDf
        
        
    '''Takes in a dataframe, puts out a dataframe that creates a bag of word representation for tags. it also 
    produces counts for the munber of users for each tag.'''          
    def tags_to_BOG(df):
        #drop empty tags
        #df.dropna(subset=['tags'])
        
        
        tagsList = [] # list of tags
        tag_tuple = ()   #tuples
        tag_tuple_list = []   #list of tuples

        
        
        #itterate over the dataframe to get info
        for row in df.itertuples(index=False):
            #These positions can be adjusted for a different column configuration
            owner = str(row[5])
            owner2 = owner + ' '
            tags = str(row[8]).lower()
            locCode = row[10]
            lat = str(row[3])
            lon = str(row[4])
            coord = lat + ' ' + lon + ' '
            #store the tags individualiy in a list
            tagsList = tags.split()
            
            
            #create a tuple for each tag
            for tags in tagsList:
                tag_tuple = (tags, owner2, locCode, coord)
                tag_tuple_list.append(tag_tuple)
                
        #  create a dictionary add the first value before the loop,
        tag_dict = {}
        #       Key        tag    owner        owner      local     local       visitor     vis        
        #                 count   list         count      list      count       list        count
        tag_dict['test'] = [0,'10120458@test', 0, '10120458@test',    0,  '10120458@test',   0,  ]
        
        #loop through the list of tuples (tags, owner, loccode) and create the dictionary
        for tuple in tag_tuple_list:
            #if tag is already in dictionary update the counts and lists
            if tuple[0] in tag_dict:
                tag_dict[tuple[0]] [0] += 1 #add to tag count
                # if owner not in ownerlist, update
                if tuple[1] not in tag_dict[tuple[0]] [1]:
                    tag_dict[tuple[0]] [2] += 1 # add to the owner count
                    tag_dict[tuple[0]] [1] += tuple[1]
                # update locals
                if tuple[2] == 'l':
                    # if owner not local list, update
                    if tuple[1] not in tag_dict[tuple[0]] [3]:
                        tag_dict[ tuple[0]] [4] +=1 # add to local count
                        tag_dict[tuple[0]] [3] += tuple[1] #add owner to the list
                # if owner not in visitor list, update
                else:
                    if tuple[1] not in tag_dict[tuple[0]] [5]:
                     tag_dict[ tuple[0]] [6] +=1 # add to visitor count
                     tag_dict[tuple[0]] [5] += tuple[1] # add to visitor list 
            # tag is not in the dictionary, it must be added         
            else:
                if tuple[2] == 'l':
                    tag_dict[tuple[0]] =[1, tuple[1], 1, tuple[1], 1,' ', 0] #add an entry for local

                else:
                    tag_dict[tuple[0]] = [1, tuple[1] ,1, ' ', 0, tuple[1], 1,]# add an entry for visitor
                    
                    
        #convert to dataframe           
        tagsDf = pd.DataFrame.from_dict(tag_dict ,orient = 'index', columns = ['tags count', 'owner', ' owner count', 'locals',
                                                                                'local count','visitors','visitor count']  )  
        return tagsDf
        
            
        
                    
    '''only makes a row for a user, latitude. longitude combination once, if the users has multiple photos they
    they are recorded in a column'''
    def userDensity( df):
        userTuple = ()
        # create the dictionary with a dummy entry
        owner = 'test'
        lat =  44.127732
        lon = -68.8749
    
        userDict = {}
        #                                                     loc code
                                   #lat        lon      owner       photo count
        userDict[owner,lat,lon] =[ 44.127732, -68.8749, 'test', 'l', 0]
        
        #itterate through the dataframe adding an entry to the dictionary or to the entry count
        for row in df.itertuples(index=False):
            lat = row[3]               
            lon = row[4]
            owner = str(row[5])
            locCode = str(row[10])
            userTuple = (owner,lat,lon)
            
            # if usertuple already in the dictionary, add to the photo count
            if userTuple in userDict:
                userDict[userTuple] [4] += 1 #add to tag count
            #if usertuple not in the dictionary, add it 
            else:
                userDict[userTuple] = [lat,lon, owner, locCode,1]
                
        #convert to dataframe           
        tagsDf = pd.DataFrame.from_dict(userDict ,orient = 'index', columns = ['latitude', 'longitude', 'owner', ' location code',
                                                        'photo count']  )  
        
        return tagsDf
        
        
                
     
    
    
                    
    
if __name__ == '__main__':
    
    
    
    '''Sample code is below'''
    
    #read in a csv, call get_tag_info, output the results to a csv
    #DF = pd.read_csv('Faroe2021_Loc_finished.csv')
    #DF2 = BOG.get_tag_info(DF,'faroeislands')
    #F2.to_csv('FaroeTagsTest.csv')
   
    
    
    #read in a csv, call tags_to_BOG, output the results as a csv
    #DF = pd.read_csv('Faroe2021_Loc_finished.csv')
    #DF2 = BOG.tags_to_BOG(DF)
    #DF2.to_csv('FaroeTest.csv')
    
    
   
