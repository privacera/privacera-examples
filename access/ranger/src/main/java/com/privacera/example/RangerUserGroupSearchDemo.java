package com.privacera.example;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class RangerUserGroupSearchDemo {

  // User and group search endpoints are not available on RangerClient; use XUser REST API directly.
  private static final String USERS_SEARCH_PATH = "/service/xusers/users";
  private static final String GROUPS_SEARCH_PATH = "/service/xusers/groups";

  public static void main(String[] args) throws Exception {

    Options options = new Options();

    Option host = OptionBuilder.hasArgs(1).isRequired().withLongOpt("host").withDescription("hostname").create('h');
    Option auth = OptionBuilder.hasArgs(1).isRequired().withLongOpt("authType").withDescription("Authentication Type")
        .create('k');
    Option user = OptionBuilder.hasArgs(1).isRequired().withLongOpt("user").withDescription("username").create('u');
    Option pass = OptionBuilder.hasArgs(1).isRequired().withLongOpt("pass").withDescription("password").create('p');
    // Optional for SSL configuration
    Option conf = OptionBuilder.hasArgs(1).withLongOpt("config").withDescription("configuration").create('c');

    options.addOption(host);
    options.addOption(auth);
    options.addOption(user);
    options.addOption(pass);
    options.addOption(conf);

    CommandLineParser parser = new BasicParser();
    CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }

    String hostName = cmd.getOptionValue('h');
    String userName = cmd.getOptionValue('u');
    String password = cmd.getOptionValue('p');
    String cfg = cmd.getOptionValue('c');
    String authType = cmd.getOptionValue('k');

    RangerAdminRestHelper restHelper = new RangerAdminRestHelper(hostName, authType, userName, password, cfg);

    // Search Users — filter by partial user name (name query param)
    Map<String, String> userQuery = new HashMap<>();
    userQuery.put("startIndex", "0");
    userQuery.put("pageSize", "25");
    userQuery.put("name", "user1");

    String usersJson = restHelper.get(USERS_SEARCH_PATH, userQuery);

    JsonObject usersResponse = JsonParser.parseString(usersJson).getAsJsonObject();
    JsonArray users = usersResponse.getAsJsonArray("vXUsers");
    int userCount = users == null ? 0 : users.size();
    System.out.println("Search users (name=user1): found " + userCount + " users");
    if (users != null) {
      for (int index = 0; index < users.size(); index++) {
        JsonObject userObject = users.get(index).getAsJsonObject();
        System.out.println("  id=" + userObject.get("id") + ", name=" + userObject.get("name"));
      }
    }

    // Search Groups — filter by partial group name (name query param)
    Map<String, String> groupQuery = new HashMap<>();
    groupQuery.put("startIndex", "0");
    groupQuery.put("pageSize", "25");
    groupQuery.put("name", "public");

    String groupsJson = restHelper.get(GROUPS_SEARCH_PATH, groupQuery);

    JsonObject groupsResponse = JsonParser.parseString(groupsJson).getAsJsonObject();
    JsonArray groups = groupsResponse.getAsJsonArray("vXGroups");
    int groupCount = groups == null ? 0 : groups.size();
    System.out.println("Search groups (name=public): found " + groupCount + " groups");
    if (groups != null) {
      for (int index = 0; index < groups.size(); index++) {
        JsonObject groupObject = groups.get(index).getAsJsonObject();
        System.out.println("  id=" + groupObject.get("id") + ", name=" + groupObject.get("name"));
      }
    }
  }
}
