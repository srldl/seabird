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
    host = Couch::Config::HOST2
    port = Couch::Config::PORT2
    password = Couch::Config::PASSWORD2
    user = Couch::Config::USER2

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


    for i in 0..col_size-1 do
         @colony_row = colony_hash[i]

         #Find the values for the colony object
          @conservation_type = nil
         #need to find the Colonyconservation connected to the countID number
         for n in 0..colonyconservation_hash.size-1 do
                @colonyconservation_row = colonyconservation_hash[n]
                if @colonyconservation_row[:colonyID].eql? @colony_row[:ColonyID]
                      @conservation = @colonyconservation_row[:conservationID]
                      numb = (@conservation.to_i) -1
                      @conservation_type = conservationtype_hash[numb]
                end
          end

          @island_obj = nil
          #If islandID is 0, there is no inlandinfo commected
          if (@colony_row[:IslandID]).to_i > 0
              island_numb = @colony_row[:IslandID]
              @island_row = islandinfo_hash[island_numb.to_i-1]
              @island_obj = {
                 :access_id => @island_row[:access_id],
                 :island => @island_row[:island],
                 :size => @island_row[:size] > 0 ? @island_row[:size] : nil,
                 :archipelago => @island_row[:archipelago] == "-1" ? @island_row[:archipelago] : nil
              }

              #remove nil values
              @island_obj.reject! {|k,v| v.nil?}
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
                   :authors => @ref_row[:authors],
                   :title => @ref_row[:title],
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


         #Store index for all matching count entries
         @count_arr = []

         #Need to find the matching entries in the count database
         for j in 0..count_size-1 do
            @count_row = count_hash[j]

            #If colony match the colony where the counting was carried out..
            if @colony_row[:ColonyID].eql? @count_row[:colonyID]

              #Create a new object - start with reference
              @reference_obj = nil
              @refID = @count_row[:CountReference]
              for r in 0..reference_hash.size-1 do
                @ref_row = reference_hash[r]
                if @ref_row[:refID].eql? @refID.to_s
                 @reference_obj = {
                   :ref_id => @ref_row[:refID],
                   :authors => @ref_row[:authors],
                   :title => @ref_row[:title],
                   :year => @ref_row[:year],
                   :volume => @ref_row[:volume].to_i > 0 ? @ref_row[:volume]:nil,
                   :pages => @ref_row[:pages],
                   :journal => @ref_row[:journal]
                }

                  #remove nil values
                  @reference_obj.reject! {|k,v| v.nil?}
               end
              end


               #Create a new object - find persons
               @people_arr = []
               #Get the countID, one number
               @count_countID = @count_row[:countID]

               #need to find the persons connected to the countID number without SQL
               for k in 0..colonycountobserver_hash.size-1 do
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

               #Create a new object and insert into object array
               @count_obj = {
                :access_id => @count_row[:countID],
                :species => @count_row[:speciesID],
                :start_date => @count_row[:StartDate],
                :end_date => @count_row[:EndDate],
                :mean => @count_row[:mean],
                :max => @count_row[:max].to_i > 0 ? @count_row[:max] : nil,
                :min => @count_row[:min].to_i > 0 ? @count_row[:min] : nil,
                :accuracy => (@count_row[:accuracy]).to_i == 1 ? 'Exactly' : 'Rough estimate',
                :unit => @unit_res[:unit],
                :method => @methods_res[:methodsName],
                :platform => @platform_res[:platform],
                :breeding => @breeding_res[:phase],
                :useful => @count_row[:useful],
                :reference => @reference_obj,
                :comment => @count_row[:comments],
                :people => @people_arr == [] ? nil : @people_arr
              }

                #remove nil values
                @count_obj.reject! {|k,v| v.nil?}

              @count_arr << @count_obj

            end  #if

         end #for


         #Create a new colony object
         @colony_obj = {
                :access_id => @colony_row[:ColonyID],
                :colony_last_update => @colony_row[:ColonyLastUpdate],
                :colony_name => @colony_row[:ColonyName1],
                :colony_alternative_name => @colony_row[:ColonyName2],
                :conservation_type => @conservation_type,
                :region => @colony_row[:regionID],
                :zone => @colony_row[:zoneID],
                :latitude => @lat_res,
                :longitude => @long_res,
                :location_accuracy => @colony_row[:LocationAccuracy],
                :colony_type => @colony_row[:ColonyType],
                :ownership => @colony_row[:ownershipID],
                :island => @island_obj,
                :length => (@colony_row[:length]).to_i > 0? @colony_row[:length]:nil,
                :distance => (@colony_row[:distance]).to_i > -1? @colony_row[:distance]:nil,
                :distance_mainland => @colony_row[:DistanceMainland].to_i > 0? @colony_row[:DistanceMainland]:nil,
                :exposure => @colony_row[:exposure],
                :area => (@colony_row[:area]).to_i > -1? @colony_row[:area]:nil,
                :confirmed => (@colony_row[:confirmed]).to_i > -1? @colony_row[:confirmed]:nil,
                :map => @colony_row[:map],
                :catching => @colony_row[:catching],
                :reference => @reference_colony_obj,
                :comment => @colony_row[:comments],
                :geometry => @geometry,
                :count => @count_arr == [] ? nil : @count_arr,
                #:histrorical_colony => @colony_row[:HistoricalComment],
                :colony_area => (@colony_row[:MultiPoints])? false:true,
                :created => timestamp,
                :updated => timestamp,
                :created_by => user,
                :updated_by => user
        }

    #remove nil values
    @colony_obj.reject! {|k,v| v.nil?}

    #Post coursetype
    doc = @colony_obj.to_json

    res2 = server.post("/"+ Couch::Config::COUCH_SEABIRD + "/", doc, user, password)
  #  puts res.body


 end #colony



end #class
end #module
