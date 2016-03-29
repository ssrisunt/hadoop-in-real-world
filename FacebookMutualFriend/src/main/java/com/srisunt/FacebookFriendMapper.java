package com.srisunt;



import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.*;


/**
 * Created by ssrisunt on 3/28/16.
 */
public class FacebookFriendMapper extends Mapper<LongWritable,Text, FriendPair,FriendArray> {


    Logger log = Logger.getLogger(FacebookFriendMapper.class);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        StringTokenizer st = new StringTokenizer(value.toString(),"\t");



        String person = st.nextToken();
        log.info(person+" per:");
        String friends = st.nextToken();
        log.info(friends+" per:");

        Friend f1 = populateFriend(person);
        List<Friend> friendList = populateFriendList(friends);
        Friend[] friendArray = Arrays.copyOf(friendList.toArray(),friendList.toArray().length,Friend[].class);
        FriendArray farray = new FriendArray(Friend.class,friendArray);

        for (Friend f2: friendList) {
            FriendPair fpair = new FriendPair(f1,f2);
            context.write(fpair,farray);
            log.info(fpair+"....."+ farray);
        }
    }

    private List<Friend> populateFriendList(String friends) {
        JSONParser parser = new JSONParser();
        List<Friend> friendList = new ArrayList<Friend>();
        try {
            JSONArray jsonArray = (JSONArray) parser.parse(friends);
            Iterator i = jsonArray.iterator();
            while (i.hasNext()) {
                JSONObject jsonObject = (JSONObject) i.next();

                Long lid = (Long)jsonObject.get("id");
                Friend friend = new Friend(new IntWritable(lid.intValue()),
                        new Text((String) jsonObject.get("name")),
                        new Text((String) jsonObject.get("hometown")));
                friendList.add(friend);
            }
        } catch (ParseException e) {
            log.error(e);
        }
        return friendList;

    }

    private Friend populateFriend(String person) {
        JSONParser parser = new JSONParser();
        Friend friend = null;
        try {
            JSONObject jsonObject = (JSONObject) parser.parse(person);
            Long lid = (Long)jsonObject.get("id");
            friend = new Friend(new IntWritable(lid.intValue()),
                    new Text((String) jsonObject.get("name")),
                    new Text((String) jsonObject.get("hometown")));

        } catch (ParseException e) {
            log.error(e);
        }
        return friend;
    }


}
