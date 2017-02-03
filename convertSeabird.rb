#!/usr/bin/env ruby
# Convert from conlony Access database to the new couchdb database
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

         puts @colony_row[:ColonyID]

         #Store index for all matching count entries
         @count_arr = []

         #Need to find the matching entries in the count database
         for j in 0..count_size-1 do
            @count_row = count_hash[j]

            #If colony match the colony where the counting was carried out..
            if @colony_row[:ColonyID].eql? @count_row[:colonyID]

              #Create a new object - start with reference
               @refID = @count_row[:CountReference]
               @ref_res = reference_hash[@refID.to_i]

               @reference_obj = nil
               unless @ref_res.nil?
                 @reference_obj = {
                   :ref_id => @ref_res[:refID],
                   :authors => @ref_res[:authors],
                   :title => @ref_res[:title],
                   :year => @ref_res[:year],
                   :volume => @ref_res[:volume].to_i > 0 ? @ref_res[:volume]:nil,
                   :pages => @ref_res[:pages],
                   :journal => @ref_res[:journal]
                 }
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
                :unit => @count_row[:unitID],   #fix
                :method => @count_row[:methodID],   #fix
                :platform => @count_row[:platformID],   #fix
                :breeding => @breeding_res[:phase],
                :useful => @count_row[:useful],
                :reference => @reference_obj,
                :comment => @count_row[:comments],
                :people => @people_arr
              }

              @count_arr << @count_obj

            end  #if

         end #for

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
          end


         #Conversion lat/long with degrees and decimalminutes into decimaldegrees
         long = @colony_row[:longitude]
         @long_res = (long[0..1]).to_f + ( long[2..6].to_f/60000 )
         if long[7] == "W" then longdecimal = "-" + longdecimal end

         lat = @colony_row[:latitude]
         @lat_res = (lat[0..1]).to_f + ( lat[2..6].to_f/60000 )


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
                :catching => @colony_row[:caching],
                :reference => nil, #fix @reference_obj
                :comment => @colony_row[:comments],
                :geometry => nil, #fix @geometry
                :count => @count_arr,
                :histrorical_colony => @colony_row[:HistoricalComment],
                :colony_area => (@colony_row[:ColonyArea]).to_i > -1? @colony_row[:ColonyArea]:nil,
                :created => timestamp,
                :updated => timestamp,
                :created_by => user,
                :updated_by => user
        }

    puts @colony_obj

    #Post coursetype
 #   doc = @colony_obj.to_json

  #  res = server.post("/"+ Couch::Config::COUCH_SEABIRD + "/", doc, user, password)
  #  puts res.body


 end #colony

end #class
end #module
