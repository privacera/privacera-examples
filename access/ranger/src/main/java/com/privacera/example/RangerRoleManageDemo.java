package com.privacera.example;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.ranger.RangerClient;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.admin.client.datatype.RESTResponse;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.util.GrantRevokeRoleRequest;
public class RangerRoleManageDemo {

  private static final String RANGER_SERVICE = "privacera_s3";
  private static final String DEMO_ROLE_NAME = "EXAMPLE_DEMO_ROLE";
  private static final String DEMO_USER = "user1";

  public static void main(String[] args) throws RangerServiceException {

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

    // Create Ranger client using the hostname, authentication type, username, password and configuration file
    RangerClient rangerClient = new RangerClient(hostName, authType, userName, password, cfg);

    // Create Role
    RangerRole roleToCreate = new RangerRole();
    roleToCreate.setName(DEMO_ROLE_NAME);
    roleToCreate.setDescription("Example role created by RangerRoleManageDemo");
    roleToCreate.setUsers(Collections.emptyList());
    roleToCreate.setGroups(Collections.emptyList());
    roleToCreate.setRoles(Collections.emptyList());

    RangerRole createdRole = rangerClient.createRole(RANGER_SERVICE, roleToCreate);
    System.out.println("Created role: id=" + createdRole.getId() + ", name=" + createdRole.getName());

    // Search Roles — filter by partial role name
    Map<String, String> roleFilter = new HashMap<>();
    roleFilter.put("roleNamePartial", DEMO_ROLE_NAME);
    roleFilter.put("startIndex", "0");
    roleFilter.put("pageSize", "100");

    List<RangerRole> matchingRoles = rangerClient.findRoles(roleFilter);
    if (CollectionUtils.isNotEmpty(matchingRoles)) {
      System.out.println("Found " + matchingRoles.size() + " roles matching filter:");
      for (RangerRole role : matchingRoles) {
        System.out.println("  id=" + role.getId() + ", name=" + role.getName());
      }
    }

    // Add User to Role — grantRole assigns the user to the target role via GrantRevokeRoleRequest
    GrantRevokeRoleRequest grantRequest = new GrantRevokeRoleRequest();
    grantRequest.setGrantor(userName);
    Set<String> targetRoles = new HashSet<>();
    targetRoles.add(DEMO_ROLE_NAME);
    grantRequest.setTargetRoles(targetRoles);
    Set<String> usersToGrant = new HashSet<>();
    usersToGrant.add(DEMO_USER);
    grantRequest.setUsers(usersToGrant);

    RESTResponse grantResponse = rangerClient.grantRole(RANGER_SERVICE, grantRequest);
    System.out.println("Added user '" + DEMO_USER + "' to role '" + DEMO_ROLE_NAME
        + "': statusCode=" + grantResponse.getStatusCode() + ", message=" + grantResponse.getMessage());

    // Verify role membership
    RangerRole roleWithMembers = rangerClient.getRole(DEMO_ROLE_NAME, userName, RANGER_SERVICE);
    System.out.println("Role after grant: name=" + roleWithMembers.getName()
        + ", users=" + roleWithMembers.getUsers());

    // Delete Role
    rangerClient.deleteRole(DEMO_ROLE_NAME, userName, RANGER_SERVICE);
    System.out.println("Deleted role: " + DEMO_ROLE_NAME);
  }
}
