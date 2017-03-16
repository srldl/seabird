#!/usr/bin/env ruby
# Convert from colony Access database to the new couchdb database
#
# Author: srldl
#
########################################

require '../server'
require '../config'
require 'net/http'
require 'net/ssh'
require 'net/scp'
require 'mdb'
require 'time'
require 'date'
require 'json'


module Couch

  class ConvertSeabird

    #Set server
    host = Couch::Config::HOST1
    port = Couch::Config::PORT1
    password = Couch::Config::PASSWORD1
    user = Couch::Config::USER1

    #Change the incoming string into GeoJSON object
    def self.createGeoJSON(inputStr)
        #Split ,
        i =  inputStr.gsub(/\s+/, "")
        latLng = (i).split(",")
        latlngdec_arr = []
        for q in 0..latLng.size-1 do
          #Split with |
          lat,lng = latLng[q].split("|")

          #Convert to decimal degrees ex. 77°14'22.2'''N 17°24'55.9'''E
          latdec = lat[0..1].to_f + (lat[3..4].to_f)/60 + (lat[6..9].to_f)/3600
          lngdec2 = lng[0..1].to_f + (lng[3..4].to_f)/60 + (lng[6..9].to_f)/3600
          lng[13] == "W"? lngdec=lngdec2*(-1) : lngdec=lngdec2
          latlngdec_arr << [lngdec.round(5),latdec.round(5)]

        end

        return {
                 :type => "polygon",
                 :coordinates => latlngdec_arr
         }
    end


    #Convert MS strings to iso8601
    def self.isoDate(inputMSstr)
         @colony_last_update = nil
         if  (inputMSstr)
            return inputMSstr[0..9] + "T12:00:00Z"
         else
            return nil
         end
    end

    #Clean database strings from odd Dbase chars
    def self.clean_res(inputMSStr)
         if  (inputMSStr != nil)
            return  inputMSStr.gsub(/[ì]/,'')
         else
            return nil
         end
    end


    #Get ready to put into database
    server = Couch::Server.new(host, port)

    #Timestamp
    a = (Time.now).to_s
    b = a.split(" ")
    c = b[0].split("-")
    dt = DateTime.new(c[0].to_i, c[1].to_i, c[2].to_i, 12, 0, 0, 0)
    timestamp = dt.to_time.utc.iso8601


    #Using mdb which lacks the ability to do sql search
    database = Mdb.open("./Colony.mdb")


    #Find the size of arrays before looping speeds up the loop
    col_size =  database[:Colony].size
    count_size = database[:ColonyCount].size

    #Reading into hashes speeds up work since mdb does not offer sql seach
    colony_hash = database[:Colony]
    count_hash = database[:ColonyCount]
    reference_hash = database[:Reference]
    colonycountobserver_hash = database[:ColonyCountObserver]
    people_hash = database[:Observers]
    colonyconservation_hash = database[:ColonyConservation]
    conservationtype_hash = database[:ConservationType]
    islandinfo_hash = database[:IslandInfo]
    breeding_hash = database[:Breeding]
    unit_hash = database[:Unit]
    methods_hash = database[:Methods]
    platform_hash = database[:Platform]
    region_hash = database[:Region]
    zones_hash = database[:Zones]
    positionaccuracy_hash = database[:PositionAccuracy]
    colonytype_hash = database[:ColonyType]
    ownership_hash = database[:Ownership]
    species_hash = database[:Species]


    for i in 0..col_size-1 do
         @colony_row = colony_hash[i]

         #Only transfer data with regionID = 7, aka Svalbard"
         if (@colony_row[:regionID].to_i == 7)



         #Find the values for the colony object
          @conservation_type_res = nil
         #need to find the Colonyconservation connected to the countID number
         for n in 0..colonyconservation_hash.size-1 do
                @colonyconservation_row = colonyconservation_hash[n]
                if @colonyconservation_row[:colonyID].eql? @colony_row[:ColonyID]
                      @conservation = @colonyconservation_row[:conservationID]
                      numb = (@conservation.to_i) -1
                      @conservation_type_res = conservationtype_hash[numb]
                end
          end

          @island_obj = nil
          #If islandID is 0, there is no inlandinfo commected

          if (@colony_row[:IslandID]).to_i > 0
              island_numb = @colony_row[:IslandID]

              @island_obj = islandinfo_hash[island_numb.to_i-1]
              #remove nil values
              #@island_obj.reject! {|k,v| v.nil?}
          end


         #Conversion lat/long with degrees and decimalminutes into decimaldegrees
         long = @colony_row[:longitude]
         @long_res = (long[0..1]).to_f + ( long[2..6].to_f/60000 )
         if long[7] == "W" then longdecimal = "-" + longdecimal end

         lat = @colony_row[:latitude]
         @lat_res = (lat[0..1]).to_f + ( lat[2..6].to_f/60000 )


         #Get colony reference
         @reference_colony_obj = nil
         @ref_colonyID = @colony_row[:Reference]
         for r in 0..reference_hash.size-1 do
              @ref_row = reference_hash[r]
              if @ref_row[:refID].eql? @ref_colonyID.to_s
                 @reference_colony_obj = {
                   :ref_id => @ref_row[:refID],
                   :authors => clean_res(@ref_row[:authors]),
                   :title => clean_res(@ref_row[:title]),
                   :year => @ref_row[:year],
                   :volume => @ref_row[:volume].to_i > 0 ? @ref_row[:volume]:nil,
                   :pages => @ref_row[:pages],
                   :journal => @ref_row[:journal]
                 }

                  #remove nil values
                  @reference_colony_obj.reject! {|k,v| v.nil?}
              end
         end

         #Get geoJSON object
         @geometry = nil
         unless @colony_row[:MultiPoints].nil?
           @geometry = createGeoJSON(@colony_row[:MultiPoints])
         end

         #Change to ISOdate
         @colony_last_update = isoDate(@colony_row[:ColonyLastUpdate])

         #regionID converted
         @regionID =  @colony_row[:regionID]
         @region_res = region_hash[(@regionID.to_i)-1]

         #zoneID converted
         @zoneID =  @colony_row[:zoneID]
         @zone_res = zones_hash[(@zoneID.to_i)-1]


          #zoneID converted
         @locationaccuracyID =  @colony_row[:LocationAccuracy]
         @positionaccuracy_res = positionaccuracy_hash[(@locationaccuracyID.to_i)-1]

         #colonytypeID converted
         @colonytypeID =  @colony_row[:ColonyType]
         @colonytype_res = colonytype_hash[(@colonytypeID.to_i)-1]

         #ownershipID converted
         @ownershipID =  @colony_row[:ownershipID]
         @ownership_res = ownership_hash[(@ownershipID.to_i)-1]

        #Store index for all matching count entries
       #  @count_arr = []

         #Need to find the matching entries in the count database

 for j in 0..count_size-1 do
            @count_row = count_hash[j]

            #If colony match the colony where the counting was carried out..
            if @colony_row[:ColonyID].eql? @count_row[:colonyID]

=begin        #Create a new object - start with reference
              @reference_obj = nil
              @refID = @count_row[:CountReference]
              for r in 0..reference_hash.size-1 do
                @ref_row = reference_hash[r]
                if @ref_row[:refID].eql? @refID.to_s
                 @reference_obj = {
                   :ref_id => @ref_row[:refID],
                   :authors => clean_res(@ref_row[:authors]),
                   :title => clean_res(@ref_row[:title]),
                   :year => @ref_row[:year],
                   :volume => @ref_row[:volume].to_i > 0 ? @ref_row[:volume]:nil,
                   :pages => @ref_row[:pages],
                   :journal => @ref_row[:journal]
                }

                  #remove nil values
                  @reference_obj.reject! {|k,v| v.nil?}
               end
              end
=end

               #Create a new object - find persons
      #         @people_arr = []
               #Get the countID, one number
         #      @count_countID = @count_row[:countID]

               #need to find the persons connected to the countID number without SQL
=begin               for k in 0..colonycountobserver_hash.size-1 do
                   @colcount_row = colonycountobserver_hash[k]
                   if @colcount_row[:CountID].eql? @count_countID
                       @count_observerID = @colcount_row[:ObserverID]  #Countobserver is  personID

                       for m in 0..people_hash.size-1 do
                            @people_row = people_hash[m]
                         if @people_row[:ObserverID].eql? @count_observerID

                             @people_obj =  {
                               :obs_id => @people_row[:ObserverID],
                               :first_name => @people_row[:FirstName],
                               :last_name => @people_row[:LastName],
                               :address => @people_row[:Address],
                               :postal_code => @people_row[:PostalCode],
                               :country => @people_row[:Country],
                               :phone => @people_row[:Telephone]
                            }

                            #remove nil values
                            @people_obj.reject! {|k,v| v.nil?}

                            @people_arr << @people_obj
                         end
                        end
                    end
                end
=end

                #BreedingID converted
                @breedingID =  @count_row[:breedingID]
                @breeding_res = breeding_hash[(@breedingID.to_i)-1]

                #Unit converted
                @unitID =  @count_row[:unitID]
                @unit_res = unit_hash[(@unitID.to_i)-1]

                #Unit converted
                @methodID =  @count_row[:methodID]
                @methods_res = methods_hash[(@methodID.to_i)-1]

                #Platform converted
                @platformID =  @count_row[:platformID]
                @platform_res = platform_hash[(@platformID.to_i)-1]

                #Change to ISOdate
                @start_date = isoDate(@count_row[:StartDate])
                @end_date = isoDate(@count_row[:EndDate])

                #species
                @speciesID =  @count_row[:speciesID]

                @species_res = species_hash[(@speciesID.to_i)-1]

               #Create a new object and insert into object array
               @count_obj = {
                :access_id => @count_row[:countID],
                :species => @species_res[:English_name],
                :start_date => @start_date,
                :end_date => @end_date,
                :mean => @count_row[:mean].to_i > 0 ? @count_row[:mean].to_i : nil,
                :max => @count_row[:max].to_i > 0 ? @count_row[:max].to_i : nil,
                :min => @count_row[:min].to_i > 0 ? @count_row[:min].to_i : nil,
                :accuracy => (@count_row[:accuracy]).to_i == 1 ? 'exactly' : 'rough estimate',
                :unit => @unit_res[:unit].downcase,
                :method => @methods_res[:methodsName].downcase,
                :platform => @platform_res[:platform],
                :breeding => @breeding_res[:phase].downcase,
                :useful => @count_row[:useful] == 1 ? true : false,
               #:reference => @reference_obj,
                :count_comment =>clean_res(@count_row[:comments]),
              #  :people => @people_arr == [] ? nil : @people_arr
              }

                #remove nil values
                @count_obj.reject! {|k,v| v.nil?}

             # @count_arr << @count_obj

         #Create a new colony object
         @colony_obj = {
                :colony_reference => @reference_colony_obj,
                :collection => 'seabird-colony',
                :lang => 'en',
                :access_id => @colony_row[:ColonyID],
                :colony_last_update => @colony_last_update,   #to datexxxx
                :colony_name => @colony_row[:ColonyName1],
                :colony_alternative_name => @colony_row[:ColonyName2],
                :conservation_type => @conservation_type_res != nil ? @conservation_type_res[:type] : nil,
                :region => @region_res[:regionname],
                :zone => @zone_res[:zonename],
                :latitude => @lat_res.round(3),
                :longitude => @long_res.round(3),
                :location_accuracy => @positionaccuracy_res[:PositionAccuracy],
                :colony_type => @colonytype_res[:ColonyType].downcase,
                :ownership => @ownership_res[:ownership].downcase,
                :length => (@colony_row[:length]).to_i > 0? ((@colony_row[:length]).to_i) : nil,
                :distance => (@colony_row[:distance]).to_i > -1? (@colony_row[:distance]).to_i : nil,
                :distance_mainland => @colony_row[:DistanceMainland].to_i > 0? @colony_row[:DistanceMainland].to_i : nil,
                :exposure => @colony_row[:exposure],
                :area => (@colony_row[:area]).to_i > -1? @colony_row[:area]:nil,
                :confirmed => (@colony_row[:confirmed]).to_i > -1? @colony_row[:confirmed] : nil,
                :map => @colony_row[:map],
               # :catching => @colony_row[:catching] == 1 ? true:false,
                :comment => clean_res(@colony_row[:comments]),
                :geometry => @geometry,
               # :historical_colony => @colony_row[:HistoricalColony], #ADD THIS NEXT TIME
                :colony_area => (@colony_row[:MultiPoints])? false:true,
                :created => timestamp,
                :updated => timestamp,
                :created_by => user,
                :updated_by => user,
                 #:count => @count_obj == [] ? nil : @count_obj,
                :access_id => @count_obj[:access_id] ? @count_obj[:access_id] : nil,
                :species =>  @count_obj[:species] ? @count_obj[:species] : nil,
                :start_date => @count_obj[:start_date] ? @count_obj[:start_date] : nil,
                :end_date => @count_obj[:end_date] ? @count_obj[:end_date] : nil,
                :mean => @count_obj[:mean]  ? @count_obj[:mean] : nil,
                :max => @count_obj[:max] ? @count_obj[:max] : nil,
                :min => @count_obj[:min] ? @count_obj[:min] : nil,
                :accuracy => @count_obj[:accuracy] ? @count_obj[:accuracy] : nil,
                :unit => @count_obj[:unit] ? @count_obj[:unit] : nil,
                :method => @count_obj[:method] ? @count_obj[:method] : nil,
                :platform => @count_obj[:platform] ? @count_obj[:platform] : nil,
                :breeding => @count_obj[:breeding] ? @count_obj[:breeding] : nil,
                :useful => @count_obj[:useful] ? false : true, #Switch
                :reference => @count_obj[:reference] ? @count_obj[:reference] : nil,
                :count_comment => @count_obj[:count_comment] ? @count_obj[:count_comment] : nil,
                :island => @island_obj ? @island_obj[:island] :nil,
                :island_size => @island_obj ? @island_obj[:size] : nil,
                :island_archipelago => @island_obj ? @island_obj[:archipelago] : nil
        }


    #remove nil values
    @colony_obj.reject! {|k,v| v.nil?}

    #Post coursetype
    doc = @colony_obj.to_json

    res2 = server.post("/"+ Couch::Config::COUCH_SEABIRD + "/", doc, user, password)

end #if
end #count
           end  #if
    end #for



end #class
end #module
