import json 
import pandas as pd
import flickrapi

# Project requires the flickrapi module developed by Steven Struval, available for download on PYPI
# Written by colleen Metcalf
class FlickrResearch:
    # constructor takes in the parameters needed for the api call
    # @Param
    # string minDateTaken = minimum date for photos in the form YYYY-MM-DD
    # string maxDateTaken = date for photos in the form YYYY-MM-DD
    # string boundingBox = 'longitude,latitude,longitude,latitude' coordintes
    # for lower left corner and upper right corner.
    # int hasGeo 1 for include geo, 0 for exclude geo
    # string extras list of extras to be included in the call see flickr's API for options
    def __init__(self, minDateTaken, maxDateTaken, boundingBox, hasGeo, extras):
        self.minDate = minDateTaken
        self.maxDate = maxDateTaken
        self.bnbox = boundingBox
        self.geo = hasGeo
        self.extras = extras



        # Function to call flickr.photos.search with a bounding box n times, retrieves 
        # one page of the request
    def bbox(self, n):
        # info needed to make a call to the API
        api_key = '3930317fd858d0bd7de3c0f1e3ec7a41'.encode(encoding='UTF-8', errors='strict')
        api_secret = 'cc00e6764b7a695c'.encode(encoding='UTF-8', errors='strict')  # convert to unicode
        flickr = flickrapi.FlickrAPI(api_key, api_secret, format='json')
        # call the flickr.photos.search function

        photoData = flickr.photos.search(min_taken_date=self.minDate,
                                         max_date_taken=self.maxDate,
                                         bbox=self.bnbox,
                                         has_geo=self.geo,
                                         extras=self.extras,
                                         page=n + 1)

        # decode and return the result
        result = json.loads(photoData.decode('utf-8'))
        return result
    
    
    
    # create a dataframe to store the data from the API call
    # This method calls and stores 16 pages
    # @return dataframe
    def createDataFrame(self):
        # list of names to assign to the multple data frames
        names = ['D0','D1','D2','D3','D4','D5','D6','D7','D8','D9','D10',
                 'D11','D12','D13','D14','D15','D16']
        # List to store the dataframes
        pageList = []
        print("Creating dataframe",end=" ")
        for i in range(16):
            print("*",end=" ")
            #request page info
            infoRequest =self.bbox(i)
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
    
    
    
    # @in dataFrame
    # @in file name 
    # Takes in a dataFrame and a file name and writes out to a csv file
    def dataFrameToCsv(self, dataFrame, fileName):
        self.df = dataFrame
        # use list funtions to simulate a stringBuilder
        textConcat = []
        textConcat.append(fileName)
        textConcat.append('.csv')
        finishedName = ''.join(textConcat)
        
        self.df.to_csv(finishedName)
        print("file is ready")
            
            
    # @in userId
    # @return user location     
    # calls flickr.people.getinfo and retrieves user location from the API, 
    # if no location is provided it returns 'none'
   
    def retrieveUserInfo(self, userId):
        #info needed to make a call to the API
        api_key = '3930317fd858d0bd7de3c0f1e3ec7a41'.encode(encoding='UTF-8', errors='strict')
        api_secret = 'cc00e6764b7a695c'.encode(encoding='UTF-8', errors='strict')  # convert to unicode
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
        
        
        
        
        

    # returns info about an inquiry, total pages in the request, photos per page, and total photos
    # use to get a sense of how many photos may be
    # available with a specific inquiry
    def requestInfo(self):
        # specify page number
        stats = self.bbox(1)
        # get the page number
        pages = stats['photos']['pages']
        # get the total of photos per page
        perPage = stats['photos']['perpage']
        # get the total number of photos
        totalPhotos = stats['photos']['total']
        return pages, perPage, totalPhotos
    
    
    # @in dataframe
    # @out int number of users
    # provides the number of unique users in a dataframe
    def NumOfUsers(self, dataFrame):
        # make a copy of the dataframe with only the owner column
        df = dataFrame[['owner']].copy()
        #drop duplicate owners
        df.drop_duplicates(keep = 'first', inplace = True)
        # count owners
        numUsers = df.count()
        
        return (numUsers)
    
    


    # @in dataframe 
    # @out dataframe with a column that includes user location
    # Funtion gets user location from the api, and adds it as an additional column
    # to the provided dataframe
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
    
     
    # @in dataframe
    # @out number of duplicates dropped
    # drops duplicates based on owner, latitude. longitiude
    def dropDuplicates(self, dataFrame):
        originalLength = len(dataFrame)
        # drop duplicates based on owner, latitude, longitude
        dataFrame.drop_duplicates(subset = ['owner', 'latitude', 'longitude'],
                                  inplace = True, keep ='last')
        newLength = len(dataFrame)
        numOfDup = originalLength - newLength
        # out put how many duplicates were dropped
        print('Duplicates dropped =', numOfDup)
        
        return dataFrame
    
    
    
    # @in dataframe
    # @out finished dataframe
    # drops all the rows that have 'none' for a location value
    
    def dropNone(self, dataFrame):
        #store all the rows where the column location = none
        dropIndex = dataFrame[dataFrame ['location'] == 'none'].index    
        #drop the rows
        dataFrame.drop(dropIndex, inplace = True)
        #reset the index
        finDf = dataFrame.reset_index(drop=True) 
    
        return finDf
    
    
    # @in int for desired number of photos 
    # @out finished data frame
    # if a dataframe larger than 4,000 photos is needed, n = 2 for 8,000 photos
    # n = 3 for 12,000, n = 4 for 16,000 photos... n = 10 for 40,000 photos
    def createLargeDataFrame(self,n):
        # list of names to store the multiple dataframes
        names = ['D1', 'D2', 'D3', 'D4', 'D5', 'D6', 'D7', 'D8', 'D9', 'D10']
        #list to store the dataframes
        pageList = []
        
        for i in range(1,n):
            #call the createDataFrame function
            names[i] = self.createDataFrame()
            # add the dataframe to a list
            pageList.append(names[i])
        
        #combine all dataframes into one
        largeDf = pd.concat(pageList, axis = 0, ignore_index = 'true',)
        
        return largeDf
        
            

# driver code your code to create objects and call methods goes below
# create flickrAPI object
myapiCall = FlickrResearch('2019-01-01', '2019-12-31', '-71.298,43.633,-70.227,43.677',
                    '1', 'geo,date_taken,tags')
