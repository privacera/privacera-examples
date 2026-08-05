package com.privacera.example;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
import org.apache.ranger.RangerClient;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.util.SearchFilter;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class RangerPolicyExportImportDemo {

  private static final String RANGER_SERVICE = "privacera_s3";
  private static final String RANGER_SERVICE_TYPE = "s3";
  // Export/Import policy endpoints are not available on RangerClient; use Ranger Admin REST API directly.
  private static final String EXPORT_PATH = "/service/plugins/policies/exportJson";
  private static final String IMPORT_PATH = "/service/plugins/policies/importPoliciesFromFile";
  private static final String DEMO_POLICY_NAME = "DemoPolicy-export-import-example-demo";

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

    // Create Ranger client using the hostname, authentication type, username, password and configuration file
    RangerClient rangerClient = new RangerClient(hostName, authType, userName, password, cfg);
    RangerAdminRestHelper restHelper = new RangerAdminRestHelper(hostName, authType, userName, password, cfg);

    // Create a policy to export
    RangerPolicy policyToCreate = generateRangerPolicy(RANGER_SERVICE_TYPE, RANGER_SERVICE, DEMO_POLICY_NAME,
        "bucket2025", "example-demo-export");
    RangerPolicy createdPolicy = rangerClient.createPolicy(policyToCreate);
    long policyId = createdPolicy.getId();
    System.out.println("Created policy for export demo: id=" + policyId + ", name=" + createdPolicy.getName());

    boolean roundTripStarted = false;
    boolean demoPolicyCleanedUp = false;
    Path exportFile = null;
    try {
      // Export Policies for the service — filter by serviceName and policyNamePartial
      Map<String, String> exportQuery = new HashMap<>();
      exportQuery.put(SearchFilter.SERVICE_NAME, RANGER_SERVICE);
      exportQuery.put(SearchFilter.POLICY_NAME_PARTIAL, DEMO_POLICY_NAME);

      String exportedJson = restHelper.get(EXPORT_PATH, exportQuery);

      JsonObject exportObject = JsonParser.parseString(exportedJson).getAsJsonObject();
      JsonArray exportedPolicies = exportObject.getAsJsonArray("policies");
      int exportedCount = exportedPolicies == null ? 0 : exportedPolicies.size();
      System.out.println("Exported " + exportedCount + " policies for service " + RANGER_SERVICE);

      if (!exportContainsPolicy(exportedPolicies, DEMO_POLICY_NAME)) {
        throw new IOException("Export did not include policy '" + DEMO_POLICY_NAME
            + "' for service " + RANGER_SERVICE + "; cannot demonstrate import round-trip");
      }

      exportFile = Files.createTempFile("ranger-policy-export-", ".json");
      Files.writeString(exportFile, exportedJson);
      System.out.println("Export JSON written to: " + exportFile);

      // Delete the policy before import to demonstrate round-trip (only after export is verified)
      rangerClient.deletePolicy(policyId);
      roundTripStarted = true;
      System.out.println("Deleted policy before import: id=" + policyId);

      // Import Policies from exported JSON file — updateIfExists merges policies that already exist
      Map<String, String> importQuery = new HashMap<>();
      importQuery.put(SearchFilter.SERVICE_NAME, RANGER_SERVICE);
      importQuery.put("updateIfExists", "true");

      String importResponse = restHelper.postMultipart(IMPORT_PATH, importQuery,
          "file", exportFile.getFileName().toString(), exportedJson, "application/json");
      System.out.println("Import policies response: " + importResponse);

      // Verify imported policy exists and clean up
      RangerPolicy importedPolicy = rangerClient.getPolicy(RANGER_SERVICE, DEMO_POLICY_NAME);
      System.out.println("Imported policy verified: id=" + importedPolicy.getId() + ", name=" + importedPolicy.getName());

      rangerClient.deletePolicy(importedPolicy.getId());
      demoPolicyCleanedUp = true;
      System.out.println("Deleted imported policy: id=" + importedPolicy.getId());
    } finally {
      if (!roundTripStarted) {
        deletePolicyQuietly(rangerClient, policyId);
      } else if (!demoPolicyCleanedUp) {
        deletePolicyByNameQuietly(rangerClient, RANGER_SERVICE, DEMO_POLICY_NAME);
      }
      if (exportFile != null) {
        Files.deleteIfExists(exportFile);
      }
    }
  }

  // This method generates a Ranger policy for S3 service with the specified parameters.
  public static RangerPolicy generateRangerPolicy(String serviceType, String service, String policyName,
      String bucketName, String objectPath) {
    Map<String, RangerPolicy.RangerPolicyResource> resources = generateS3Resources(bucketName, objectPath);
    RangerPolicy policy = new RangerPolicy();
    policy.setService(service);
    policy.setName(policyName);
    policy.setDescription("Policy for " + serviceType);
    policy.setIsEnabled(true);
    policy.setIsAuditEnabled(true);
    policy.setResources(resources);
    policy.setPolicyItems(getPolicyItems(Arrays.asList("user1"), Collections.emptyList(), Collections.emptyList()));
    policy.setServiceType(serviceType);
    return policy;
  }

  // This method generates a list of policy items with the given principals for the Ranger policy.
  private static List<RangerPolicy.RangerPolicyItem> getPolicyItems(List<String> users, List<String> groups,
      List<String> roles) {
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
  private static Map<String, RangerPolicy.RangerPolicyResource> generateS3Resources(String bucketName,
      String objectPath) {
    Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();
    resources.put("bucketname", new RangerPolicy.RangerPolicyResource(bucketName, false, false));
    resources.put("objectpath", new RangerPolicy.RangerPolicyResource(objectPath, false, true));
    return resources;
  }

  private static void deletePolicyQuietly(RangerClient rangerClient, long policyId) {
    try {
      rangerClient.deletePolicy(policyId);
      System.out.println("Cleaned up demo policy: id=" + policyId);
    } catch (RangerServiceException exception) {
      System.err.println("Failed to clean up demo policy id=" + policyId + ": " + exception.getMessage());
    }
  }

  private static void deletePolicyByNameQuietly(RangerClient rangerClient, String serviceName, String policyName) {
    try {
      RangerPolicy policy = rangerClient.getPolicy(serviceName, policyName);
      rangerClient.deletePolicy(policy.getId());
      System.out.println("Cleaned up demo policy: id=" + policy.getId() + ", name=" + policyName);
    } catch (RangerServiceException exception) {
      System.err.println("Failed to clean up demo policy '" + policyName + "' for service " + serviceName + ": "
          + exception.getMessage());
    }
  }

  private static boolean exportContainsPolicy(JsonArray exportedPolicies, String policyName) {
    if (exportedPolicies == null || exportedPolicies.size() == 0) {
      return false;
    }
    for (int index = 0; index < exportedPolicies.size(); index++) {
      JsonObject exportedPolicy = exportedPolicies.get(index).getAsJsonObject();
      if (exportedPolicy.has("name") && policyName.equals(exportedPolicy.get("name").getAsString())) {
        return true;
      }
    }
    return false;
  }
}
