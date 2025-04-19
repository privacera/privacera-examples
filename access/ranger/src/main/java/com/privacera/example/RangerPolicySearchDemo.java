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

    // Creating policy for bucket2025/dir1
    RangerPolicy rangerPolicy = generateRangerPolicy(RANGER_SERVICE_TYPE, RANGER_SERVICE, "bucket2025", "dir1");
    rangerPolicy = rangerClient.createPolicy(rangerPolicy);
    System.out.println("Created policy: " + rangerPolicy.getId() + " with name: " + rangerPolicy.getName());

    // Creating policy for bucket2025/dir1/dir2
    RangerPolicy rangerPolicy1 = generateRangerPolicy(RANGER_SERVICE_TYPE, RANGER_SERVICE, "bucket2025", "dir1/dir2");
    rangerPolicy1 = rangerClient.createPolicy(rangerPolicy1);
    System.out.println("Created policy: " + rangerPolicy1.getId() + " with name: " + rangerPolicy1.getName());

    //Search for policies for a given user
    Map<String, String> userFilter = new HashMap<>();
    userFilter.put("user", "user1");
    userFilter.put("serviceName", "privacera_s3");

    List<RangerPolicy> userPolicyList = rangerClient.findPolicies(userFilter);
    if (CollectionUtils.isNotEmpty(userPolicyList)) {
      System.out.println("Found " + userPolicyList.size() + " policies for user : user1");
    }

    //Search for policies for a given role
    Map<String, String> roleFilter = new HashMap<>();
    roleFilter.put("role", "ROLE_1");
    roleFilter.put("serviceName", "privacera_s3");
    List<RangerPolicy> rolePolicyList = rangerClient.findPolicies(roleFilter);
    if (CollectionUtils.isNotEmpty(rolePolicyList)) {
      System.out.println("Found " + rolePolicyList.size() + " policies for role : ROLE_1");
    }

    // Search for policies for a given resource
    Map<String, String> filter = new HashMap<>();

    // Ranger, the policyType field refers to the type of the policy.
    // 0	Access Policy (default)
    // 1	Data Masking Policy
    // 2	Row Filtering Policy
    filter.put("policyType", "0");

    //Resource fields are available in the Service definition
    filter.put("resource:bucketname", "bucket2025");
    filter.put("resource:objectpath", "dir1");
    // Ranger service name
    filter.put("serviceName", "privacera_s3");

    List<RangerPolicy> policyList = rangerClient.findPolicies(filter);
    if (CollectionUtils.isNotEmpty(policyList)) {
      // Without resourceMatchScope - Retrieves policies that exactly match the specified resource, along with wildcard policies such as default policies.
      // all - bucketname, objectpath
      // Policy for bucket2025/dir1
      System.out.println("Found " + policyList.size() + " policies for resource without 'resourceMatchScope'");
      for (RangerPolicy policy : policyList) {
        System.out.println(policy.getName());
      }
    }

    // Examples for resourceMatchScope

    // resourceMatchScope is used to match the resource
    // 'self': Retrieves policies that exactly match the specified resource.
    filter.put("resourceMatchScope", "self");
    policyList = rangerClient.findPolicies(filter);
    if (CollectionUtils.isNotEmpty(policyList)) {
      // resourceMatchScope: 'self' — Retrieves policies that exactly match the specified resource.
      // Policy for bucket2025/dir1
      System.out.println("Found " + policyList.size() + " policies for resource with 'resourceMatchScope' as self");
      for (RangerPolicy policy : policyList) {
        System.out.println(policy.getName());
      }
    }

    // resourceMatchScope: 'ancestor' — Retrieves policies where the specified resource in the filter is an ancestor of the policy resource.
    // Valid only for resources with isRecursive set to true (e.g., objectpath).
    filter.put("resourceMatchScope", "ancestor");
    policyList = rangerClient.findPolicies(filter);
    if (CollectionUtils.isNotEmpty(policyList)) {
      // Should return 1 policy
      // Policy for bucket2025/dir1/dir2
      System.out.println("Found " + policyList.size() + " policies where 'resourceMatchScope' is set to ancestor. These are policies where 'dir1' is an ancestor resource.");
      for (RangerPolicy policy : policyList) {
        System.out.println(policy.getName());
      }
    }

    // 'self_or_ancestor': Retrieves policies that either match the specified resource exactly or where the specified resource is an ancestor of the policy resource.
    filter.put("resourceMatchScope", "self_or_ancestor");
    policyList = rangerClient.findPolicies(filter);
    if (CollectionUtils.isNotEmpty(policyList)) {
      // Should return 2 policy
      // Policy for bucket2025/dir1
      // Policy for bucket2025/dir1/dir2
      System.out.println("Found " + policyList.size() + " policies with 'resourceMatchScope' set to self_or_ancestor — covering cases where 'dir1' is either the resource itself or an ancestor.");
      for (RangerPolicy policy : policyList) {
        System.out.println(policy.getName());
      }
    }
    // Clean up policies created
    rangerClient.deletePolicy(rangerPolicy.getId());
    rangerClient.deletePolicy(rangerPolicy1.getId());
  }

  // This method generates a Ranger policy for S3 service with the specified parameters.
  public static RangerPolicy generateRangerPolicy(String serviceTpe, String service, String bucketName, String objectPath) {
    Map<String, RangerPolicy.RangerPolicyResource> resources = generateS3Resources(bucketName, objectPath);
    RangerPolicy policy = new RangerPolicy();
    policy.setService(service);
    policy.setName("Policy for " + bucketName + "/" + objectPath);
    policy.setDescription("Policy for " + serviceTpe);
    policy.setIsEnabled(true);
    policy.setIsAuditEnabled(true);
    policy.setResources(resources);
    policy.setPolicyItems(RangerPolicySearchDemo.getPolicyItems(Arrays.asList("user1", "user2"), Collections.EMPTY_LIST, Arrays.asList("ROLE_1", "ROLE_2")));
    policy.setServiceType(serviceTpe);
    return policy;
  }

  // This method generates a list of policy items with the given principals for the Ranger policy.
  private static List<RangerPolicy.RangerPolicyItem> getPolicyItems(List<String> users, List<String> groups, List<String> roles) {
    RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
    policyItem.setDelegateAdmin(false);
    policyItem.setAccesses(getAccesses());
    policyItem.setUsers(users);
    policyItem.setGroups(groups);
    policyItem.setRoles(roles);
    return Arrays.asList(policyItem);
  }

  // This method generates a list of S3 accesses permissions for the Ranger policy item.
  private static List<RangerPolicy.RangerPolicyItemAccess> getAccesses() {
    RangerPolicy.RangerPolicyItemAccess readAccess = new RangerPolicy.RangerPolicyItemAccess("read", true);
    RangerPolicy.RangerPolicyItemAccess writeAccess = new RangerPolicy.RangerPolicyItemAccess("write", true);
    RangerPolicy.RangerPolicyItemAccess deleteAccess = new RangerPolicy.RangerPolicyItemAccess("delete", true);
    RangerPolicy.RangerPolicyItemAccess mReadAccess = new RangerPolicy.RangerPolicyItemAccess("mread", true);
    RangerPolicy.RangerPolicyItemAccess mWriteAccess = new RangerPolicy.RangerPolicyItemAccess("mwrite", true);
    RangerPolicy.RangerPolicyItemAccess adminAccess = new RangerPolicy.RangerPolicyItemAccess("admin", true);
    return Arrays.asList(readAccess, writeAccess, deleteAccess, mWriteAccess, mReadAccess, adminAccess);
  }

  // This method generates S3 resources for the Ranger policy.
  private static Map<String, RangerPolicy.RangerPolicyResource> generateS3Resources(String bucketName, String objectPath) {
    Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();
    resources.put("bucketname", new RangerPolicy.RangerPolicyResource(bucketName, false, false));
    resources.put("objectpath", new RangerPolicy.RangerPolicyResource(objectPath , false, true));
    return resources;
  }
}