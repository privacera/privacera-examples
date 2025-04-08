package com.privacera.example;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;

public class RangerPolicySearchDemo {

  private static final String RANGER_SERVICE = "privacera_s3";
  private static final String RANGER_SERVICE_TYPE = "s3";

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

    // Creating policy
    RangerPolicy rangerPolicy = generateRangerPolicy(RANGER_SERVICE_TYPE, RANGER_SERVICE );
    RangerPolicy savedPolicy = rangerClient.createPolicy(rangerPolicy);
    System.out.println("Created policy: " + savedPolicy.getId() + " with name: " + savedPolicy.getName());

    //Search for policies for a given user
    Map<String, String> userFilter = new HashMap<>();
    userFilter.put("user", "user1");
    userFilter.put("serviceName", "privacera_s3");
    userFilter.put("zoneName", "SZ-1");

    List<RangerPolicy> userPolicyList = rangerClient.findPolicies(userFilter);
    if (CollectionUtils.isNotEmpty(userPolicyList)) {
      System.out.println("Found " + userPolicyList.size() + " policies for user");
    }

    //Search for policies for a given role
    Map<String, String> roleFilter = new HashMap<>();
    roleFilter.put("role", "ROLE_1");
    roleFilter.put("serviceName", "privacera_s3");
    roleFilter.put("zoneName", "SZ-1");
    List<RangerPolicy> rolePolicyList = rangerClient.findPolicies(roleFilter);
    if (CollectionUtils.isNotEmpty(rolePolicyList)) {
      System.out.println("Found " + rolePolicyList.size() + " policies for role");
    }

    // Search for policies for a given resource
    Map<String, String> filter = new HashMap<>();
    filter.put("zoneName", "SZ-1");
    // Ranger, the policyType field refers to the type of the policy.
    // 0	Access Policy (default)
    // 1	Data Masking Policy
    // 2	Row Filtering Policy
    filter.put("policyType", "0");

    //Resource fields are available in the Service definition
    filter.put("resource:bucketname", "bucket2025");
    filter.put("resource:objectpath", "/data/04042025/*");
    filter.put("resourceMatchScope", "self");
    filter.put("serviceName", "privacera_s3");
    List<RangerPolicy> policyList = rangerClient.findPolicies(filter);
    if (CollectionUtils.isNotEmpty(policyList)) {
      System.out.println("Found " + policyList.size() + " policies");
    }
  }

  public static RangerPolicy generateRangerPolicy(String serviceTpe, String service) {
    Map<String, RangerPolicy.RangerPolicyResource> resources = generateResources(serviceTpe);
    RangerPolicy policy = new RangerPolicy();
    policy.setService(service);
    policy.setName("ranger-policy-example");
    policy.setDescription("Policy for " + serviceTpe);
    policy.setIsEnabled(true);
    policy.setIsAuditEnabled(true);
    policy.setResources(resources);
    policy.setZoneName("SZ-1");
    policy.setPolicyItems(RangerPolicySearchDemo.getPolicyItems(Arrays.asList("user1", "user2"), Collections.EMPTY_LIST, Arrays.asList("ROLE_1", "ROLE_2")));
    policy.setServiceType(serviceTpe);
    return policy;
  }

  private static List<RangerPolicy.RangerPolicyItem> getPolicyItems(List<String> users, List<String> groups, List<String> roles) {
    RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
    policyItem.setDelegateAdmin(false);
    policyItem.setAccesses(getAccesses());
    policyItem.setUsers(users);
    policyItem.setGroups(groups);
    policyItem.setRoles(roles);
    return Arrays.asList(policyItem);
  }

  private static List<RangerPolicy.RangerPolicyItemAccess> getAccesses() {
    RangerPolicy.RangerPolicyItemAccess readAccess = new RangerPolicy.RangerPolicyItemAccess("read", true);
    RangerPolicy.RangerPolicyItemAccess writeAccess = new RangerPolicy.RangerPolicyItemAccess("write", true);
    RangerPolicy.RangerPolicyItemAccess deleteAccess = new RangerPolicy.RangerPolicyItemAccess("delete", true);
    RangerPolicy.RangerPolicyItemAccess mReadAccess = new RangerPolicy.RangerPolicyItemAccess("mread", true);
    RangerPolicy.RangerPolicyItemAccess mWriteAccess = new RangerPolicy.RangerPolicyItemAccess("mwrite", true);
    RangerPolicy.RangerPolicyItemAccess adminAccess = new RangerPolicy.RangerPolicyItemAccess("admin", true);
    return Arrays.asList(readAccess, writeAccess, deleteAccess, mWriteAccess, mReadAccess, adminAccess);
  }

  private static Map<String, RangerPolicy.RangerPolicyResource> generateResources(String service) {
    Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();
    String resource = "bucket2025";
    switch (service) {
      case "s3": {
        resources.put("bucketname", new RangerPolicy.RangerPolicyResource(resource, false, false));
        resources.put("objectpath", new RangerPolicy.RangerPolicyResource("/data/04042025/*" , false, false));
      }
    }
    return resources;
  }
}