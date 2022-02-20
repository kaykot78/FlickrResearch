import json 
import pandas as pd
import numpy as np
import flickrapi
import re

"""
# By Colleen Metcalf, This class provides methods to create, clean, and export dataframes containing
# photo data p.
# Project requires the flickrapi module developed by Steven Struval, available for download on PYPI
"""
class FlickrResearch:
    DFList = []
    """
    # constructor takes in the parameters needed for an api call
    # @Param
    # string minDateTaken = minimum date for photos in the form YYYY-MM-DD
    # string maxDateTaken = date for photos in the form YYYY-MM-DD
    # string boundingBox = 'longitude,latitude,longitude,latitude' coordintes
    # for lower left corner and upper right corner of the box.
    # int hasGeo 1 for include geo, 0 for exclude geo
    # string extras list of extras to be included in the call see flickr's API for options
    # key and secret to access the API, you must get them from flicker
    """
    def __init__(self, minTakenDate, maxTakenDate, hasGeo, extras, yourKey, yourSecret):
        #self.minDate = minDateTaken
        #self.maxDate = maxDateTaken
        self.maxDate = maxTakenDate
        self.minDate = minTakenDate
        self.geo = hasGeo
        self.extras = extras
        self.key = yourKey
        self.secret = yourSecret
        
        
        
        
    """
    # @param dataFrame
    # @out basic stats
    # invarient- data must be sorted by owner before passing into this function
    # Are one or two users contributing signifigantly more than everbody else? this is where you find out.  
    # Possible bugs, seems ok 
    """
    def accessUsers( self, dataFrame):
        dFrame = dataFrame
        #basic stats
        
        #totalPhotos = dFrame.count()
        # set a value for previous user
        previousUser = 'Start'                 #at[0, 'owner']
        
        
        # dictionary to store the user and their number of photos
        usersAndPhotos = {}
        photoCount = 0
        
        #count photos per user, itterate over the dataframe
        for row in dFrame.itertuples(index = False):
            user = str(row[5])
            # add the user when they are first encountered
            usersAndPhotos[user] = 1
            # count photos
            if user == previousUser:
                photoCount += 1;
            #update dictionary with correct photocounts     
            else:
                if photoCount == 0:
                     usersAndPhotos[previousUser] = 1   
                
                else:
                    usersAndPhotos[previousUser] = photoCount +1
                    photoCount = 0
            
            previousUser = user
            
        newdFrame = pd.DataFrame.from_dict(usersAndPhotos, orient = 'index', columns = ['numOfPhotos'])
       
        nextDf =  newdFrame.drop(axis = 0, labels = ['Start'])
        
        return nextDf  





    """
     @param dataframe 
     @out dataframe with a column that includes user location
     Funtion gets user location from the api, and adds it as an additional column
     to the provided dataframe
    """
    def appendUserInfo(self, dataFrame):
        
        # make a copy of the dataframe with only the owner column
        ownerDf = dataFrame[['owner']].copy()
        # remove duplicate owner Ids
        ownerDf.drop_duplicates(keep = 'first', inplace = True)
        #iterate over the rows and write to a list
        userList = []
        for index, rows in ownerDf.iterrows():
            userList.append(rows.owner)
            
        #Run each user ID to obtain location
        print("Getting user info")
        #list to hold the locations
        location=[]
        for userId in userList:
            A = self.retrieveUserInfo(userId)
            print("*",end=" ")
            # add location data to the location list
            location.append(A)
            
        #add the loctions to a dataframe    
        locDf = pd.DataFrame(location, columns = ['location'])
        #add the users back to the location dataframe
        locDf.insert(1, 'owner', userList)
        #merge the location data with the original data frame
        finDf = dataFrame.merge(locDf, on = 'owner', sort = True)
        # blank line for console output
        print()
        
        return finDf
    
    
    
    
    

    """
    #calculates the average number of photos, uses 10 requests.
    """
    def averageOfPhotos(self, lonLL, latLL, lonUR, latUR):
        sum = 0
        for i in range(0,10):
            # specify page number
            stats = self.bboxWithCoord( lonLL, latLL, lonUR, latUR,1)
            # get the total number of photos
            totalPhotos = stats['photos']['total']
            sum = sum + totalPhotos
        average = sum/10
        
        return average
          
    
    """
    #bbox that uses provided coord used with numofphotos and recursivebbox
    """
    def bboxWithCoord(self, lonLL, latLL, lonUR, latUR , n ):
        #convert boundong box to string
        stringList = [str(lonLL), ",", str(latLL), ",", str(lonUR), ",", str(latUR)]
        newList = ''
        newBbox = newList.join(stringList)
        # info needed to make a call to the API
        api_key = self.key.encode(encoding='UTF-8', errors='strict')
        api_secret = self.secret.encode(encoding='UTF-8', errors='strict')  # convert to unicode
        flickr = flickrapi.FlickrAPI(api_key, api_secret, format='json')
        # call the flickr.photos.search function

        photoData = flickr.photos.search(bbox= newBbox,
                                         min_taken_date = self.minDate,
                                         max_taken_date = self.maxDate,
                                         has_geo=self.geo,
                                         extras=self.extras,
                                         page=n + 1)
        
         # decode and return the result
        result = json.loads(photoData.decode('utf-8'))
        return result
    
    
    
    """
    # create a dataframe to store the data from the API call
    # This method calls and stores 16 pages, 4,000 photos
    # @return dataframe
    """
    def createDataFrame(self,lonLL, latLL, lonUR, latUR):
        # list of names to assign to the multple data frames
        names = ['D0','D1','D2','D3','D4','D5','D6','D7','D8','D9','D10',
                 'D11','D12','D13','D14','D15','D16']
        # List to store the dataframes
        pageList = []
        print("Creating dataframe",end=" ")
        for i in range(16):
            print("*",end=" ")
            #request page info
            infoRequest =self.bboxWithCoord(lonLL, latLL, lonUR, latUR, i)
            #create a dataframe from the info request 
            names[i] = pd.DataFrame(infoRequest['photos']['photo'],
                                    columns = ['id','accuracy','latitude',
                                               'longitude','owner',
                                               'datetaken','title','tags'])
            # add dataframe to a list
            pageList.append(names[i])
        # combine all data frames into one
        flickrData = pd.concat(pageList, axis = 0, ignore_index = 'true',)
        # blank line for console output
        print()
            
        return flickrData
    
    
    
    
    
    """
     @param dataframe
     @out number of duplicates dropped
     drops duplicates based on owner, latitude. longitiude, title, tags, datetaken
    """
    def dropDuplicates(self, dataFrame):
        originalLength = len(dataFrame)
        # drop duplicates based on owner, latitude, longitude
        dataFrame.drop_duplicates(subset = ['owner', 'latitude', 'longitude', 'title', 'tags', 'datetaken'],
                                  inplace = True, keep ='last')
        newLength = len(dataFrame)
        numOfDup = originalLength - newLength
        # out put how many duplicates were dropped
        print('Duplicates dropped =', numOfDup)
        
        return dataFrame
    
    
    
    
    """
     @param dataframe
     @out finished dataframe
     drops all the rows that have 'none' for a location value
    """
    def dropNone(self, dataFrame):
        #store all the rows where the column location = none
        dropIndex = dataFrame[dataFrame ['location'] == 'none'].index    
        #drop the rows
        dataFrame.drop(dropIndex, inplace = True)
        #reset the index
        finDf = dataFrame.reset_index(drop=True) 
    
        return finDf  
    
    
    
    
    def numOfPhotos(self, lonLL, latLL, lonUR, latUR):
        # specify page number
        stats = self.bboxWithCoord( lonLL, latLL, lonUR, latUR,1)
        # get the total number of photos
        totalPhotos = stats['photos']['total']
        return totalPhotos    
    
    
    
    
    
    """
     @param dataframe
     @out int number of users
     provides the number of unique users in a dataframe
    """
    def numOfUsers(self, dataFrame):
        # make a copy of the dataframe with only the owner column
        df = dataFrame[['owner']].copy()
        #drop duplicate owners
        df.drop_duplicates(keep = 'first', inplace = True)
        # count owners
        numUsers = df.count()
        
        return (numUsers) 
    
    
    
    
  
    """
    Prefilling location codes based on country. Tests for and fill in "local" and all others are filled as 
    visitor. The test words must be adjusted inside the function. Data should be manually checked for accuracy.
    """
    def prefill(self, dataframe):
        #make a list to hold the values that will be used for np.where
        testLocList =[]
        #itterate through the dataframe
        for row in dataframe.itertuples(index = False):
            # the owner index may need to adjusted for for different files
            # use regex to remove punctuation and convert to lowercase
            location = re.sub('[^\w\s]','',row[9]).lower()
            
            #split the string 
            locationWords = location.split((' '))
            #itterate through the words in the location
            for words in locationWords:
                #test for local
                if words == "maine":
                    result = "local"
                    break
                elif words == "me":
                    result = "local"
                    break
                #elif words == "yourterm":
                    #result = "local"
                    #break
                #elif words == "yourterm":
                    #result == "local"
                    #break
                #elif words == "yourterm":
                    #result = "local"
                    #break
                #elif words == "yourterm":
                    #result = "local"
                    #break
                #elif words == "yourterm":
                    #result = "local"
                    #break
                #elif words == "yourterm":
                    #result = "local"
                    #break
                #elif words == "yourterm":
                    #result = "local"
                    #break
                   
                    
                else:
                    result = "visitor"
                   
            # fill the list             
            if result == "local":
                testLocList.append("local")
            else:
                testLocList.append("none")
    
        # add locList to the dataframe
        dataframe['test_loc']= testLocList  
        #fill in the location code with l for locals, and v for all others
        dataframe['location code'] = np.where(dataframe ['test_loc'] == 'local', 'l', 'v') 
        print(dataframe)         
        

        return dataframe    
        
        
        
  
    """
     invarient - designed for the northwestern hemisphere, north of the equater and west of the prime meriden
     @param longitude, latitude coordinates for a geographical bounding box- lower left corner(lon,lat),
     upper right corner(lon,lat). adjustments for different hemispheres would be made in BOX 1, BOX 2, BOX 3, and BOX 4
    """
    def recursiveBBox(self, lonLL, latLL, lonUR, latUR):
        stats = self.bboxWithCoord( lonLL, latLL, lonUR, latUR,1)
        # get the total number of photos
        totalPhotos = stats['photos']['total']
        print(totalPhotos)
        #varible to increment for dataframe name
        DFnum = 0
        #calculate the change in longitude
        changeLon = abs(lonLL - lonUR)/2
        #calculate the change in latitude
        changeLat = abs(latUR - latLL)/2
        
        ##calculate and check the boxes
        #BOX 1  ***adjustments for a different hemisphere are made here***
        #box 1 lower left corner
        Box1LonLL = lonLL           
        Box1LatLL = latLL + changeLat
        #box 1 upper right
        Box1LonUR = lonLL + changeLon        
        Box1LatUR = latUR
        
        #Check the original box, if less than 4,000 pull without recurision
        if(self.numOfPhotos(lonLL, latLL, lonUR, latUR) < 4000):
            DF = self.createDataFrame(lonLL, latLL, lonUR, latUR)
            return DF
        
        
        # if photo count is less than 4,000 pull photos and add to the dataframe
        
        if (self.numOfPhotos(Box1LonLL,Box1LatLL ,Box1LonUR ,Box1LatUR) < 4000):
            #create a name for the dataframe
            letter = 'D'
            # call createDataFrame
            print('Box 1')
            name = self.createDataFrame(Box1LonLL,Box1LatLL ,Box1LonUR ,Box1LatUR)
            #add the dataframe to the list
            self.DFList.append(name)
            #increment the dataframe number
            DFnum +=1
        else:
            self.recursiveBBox(Box1LonLL,Box1LatLL ,Box1LonUR ,Box1LatUR)
            
            
        #BOX 2  ***adjustments for a different hemisphere are made here***
        #box 2 lower left corner  
        Box2lonLL = lonLL + changeLon
        Box2latLL = latLL + changeLat
        #box 2 upper right
        Box2lonUR = lonUR
        Box2latUR = latUR
            
            # if photo count is less than 4,000 pull photos and add to the dataframe 
        if (self.numOfPhotos( Box2lonLL,Box2latLL,Box2lonUR,Box2latUR ) < 4000):
            #create a name for the dataframe
            letter = 'D'
            number = str(DFnum)
            name = " ".join([letter,number])
            # call createDataFrame
            print('Box 2')
            name = self.createDataFrame(Box2lonLL,Box2latLL,Box2lonUR,Box2latUR)
            #add the dataframe to the list
            self.DFList.append(name)
            #increment the dataframe number
            DFnum +=1
        else:
            self.recursiveBBox(Box2lonLL,Box2latLL,Box2lonUR,Box2latUR)
            
        
        #BOX 3   ***adjustments for a different hemisphere are made here***
        #box3 lower left   
        box3lonLL = lonLL
        box3latLL = latLL
        #box 3 upper right
        box3lonUR = lonLL + changeLon
        box3latUR = latLL + changeLat
        
        
         # if photo count is less than 4,000 pull photos and add to the dataframe 
        if (self.numOfPhotos(box3lonLL,box3latLL,box3lonUR,box3latUR ) < 4000):
            #create a name for the dataframe
            letter = 'D'
            number = str(DFnum)
            name = " ".join([letter,number])
            # call createDataFrame
            print('Box 3')
            name = self.createDataFrame(box3lonLL,box3latLL,box3lonUR,box3latUR)
            #add the dataframe to the list
            self.DFList.append(name)
            #increment the dataframe number
            DFnum +=1
        else:
            self.recursiveBBox(box3lonLL,box3latLL,box3lonUR,box3latUR)
    
        #BOX 4  ***adjustments for a different hemisphere are made here***
        #box4 lower left  
        box4lonLL = lonLL + changeLon
        box4latLL = latLL
        box4lonUR = lonUR
        box4latUR = latUR - changeLat
        
        
         # if photo count is less than 4,000 pull photos and add to the dataframe 
        if (self.numOfPhotos(box4lonLL,box4latLL,box4lonUR,box4latUR ) < 4000):
            #create a name for the dataframe
            letter = 'D'
            number = str(DFnum)
            name = " ".join([letter,number])
            # call createDataFrame
            print('Box 4')
            name = self.createDataFrame(box4lonLL,box4latLL,box4lonUR,box4latUR)
            #add the dataframe to the list
            self.DFList.append(name)
            #increment the dataframe number
            DFnum +=1
        else:
            self.recursiveBBox(box4lonLL,box4latLL,box4lonUR,box4latUR)
    
        
        
        #combine all the datatframes from the list
        
        photoData = pd.concat(self.DFList, axis = 0, ignore_index = 'true')
        
            
            
        return photoData    
    
    
    """        
     @param userId
     @return user location     
     calls flickr.people.getinfo and retrieves user location from the API, 
     if no location is provided it returns 'none'
    """
   
    def retrieveUserInfo(self, userId):
        #info needed to make a call to the API
        api_key = self.key.encode(encoding='UTF-8', errors='strict')
        api_secret = self.secret.encode(encoding='UTF-8', errors='strict')  # convert to unicode
        flickr = flickrapi.FlickrAPI(api_key, api_secret, format='json')
        
        
        userInfo = flickr.people.getinfo(user_id = userId)
        Info = json.loads(userInfo.decode('utf-8'))
        #select out and return location
        try:
            location = Info['person']['location']['_content']
            if location == "":
                return 'none'
            else:
                return location
        except:
            return 'none'   
                

                 
    '''only makes a row for a user, latitude. longitude combination once, if the users have multiple photos they
    they are recorded in a count column. This for data with owner information'''
    def userDensity(self, df):
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
    
    
    
    
    
    
    


    '''same as user density, but it works with raw data that has no location.'''
                
    def userDensityRaw(self, df):
        userTuple = ()
        # create the dictionary with a dummy entry
        owner = 'test'
        lat =  44.127732
        lon = -68.8749
    
        userDict = {}
        #                                                     
                                   #lat        lon      owner   photo count
        userDict[owner,lat,lon] =[ 44.127732, -68.8749, 'test', 0]
        
        #itterate through the dataframe adding an entry to the dictionary or to the entry count
        for row in df.itertuples(index=False):
            lat = row[3]               
            lon = row[4]
            owner = str(row[5])
            userTuple = (owner,lat,lon)
            
            # if usertuple already in the dictionary, add to the photo count
            if userTuple in userDict:
                userDict[userTuple] [3] += 1 #add to tag count
            #if usertuple not in the dictionary, add it 
            else:
                userDict[userTuple] = [lat,lon, owner,1]
                
        #convert to dataframe           
        tagsDf = pd.DataFrame.from_dict(userDict ,orient = 'index', columns = ['latitude', 'longitude', 'owner',
                                                        'photo count']  )  
        
        return tagsDf        
    


        
            
   

# driver code your code to create objects and call methods goes below


    

if __name__ == '__main__':
   
    #****Sample Code Below****

    
    # Call to the constructor
    apiCall = FlickrResearch('2021-01-01','2021-12-31', '1', 'geo,date_taken,tags',
                             'yourkey12345','yoursecret12345')



    #Read in a csv and print the number of users
    #DF = pd.read_csv('yourData.csv')
    #DF1 = apiCall.NumOfUsers(DF)
    #print(apiCall.NumOfUsers(DF))
    
    
    #read in a csv call a recursive bounding box, drop duplicates, get user location and write 
    #out to a csv
    #DF2 =apiCall.recursiveBBox(-7.791,61.37,-6.102,62.524)
    #DF3 = apiCall.dropDuplicates(DF2)
    #DF4 = apiCall.appendUserInfo(DF3)
    #DF4.to_csv('Faroe2021TTT_Loc.csv')
    
    
    # Read in a csv append location info, drop all users without location, put out a csv
    #DF = pd.read_csv('box29.csv')
    #DF2 = apiCall.appendUserInfo(DF)
    #DF3 = apiCall.dropNone(DF2)
    #apiCall.dataFrameToCsv(DF3, 'Aloc29')
    
   
    
   
  


   
   
   
   
   
   
   
   
   
   
    
   
   
   
   
   
   
   
   
   
   
   
   
    