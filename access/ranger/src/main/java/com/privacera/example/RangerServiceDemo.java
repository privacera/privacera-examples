package com.privacera.example;

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
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.util.SearchFilter;

public class RangerServiceDemo {

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

    // Search Services — filter by partial service name and service type.
    Map<String, String> serviceFilter = new HashMap<>();
    serviceFilter.put(SearchFilter.SERVICE_NAME_PARTIAL, "privacera");
    serviceFilter.put(SearchFilter.SERVICE_TYPE, RANGER_SERVICE_TYPE);
    serviceFilter.put(SearchFilter.START_INDEX, "0");
    serviceFilter.put(SearchFilter.PAGE_SIZE, "100");

    List<RangerService> services = rangerClient.findServices(serviceFilter);
    if (CollectionUtils.isNotEmpty(services)) {
      System.out.println("Found " + services.size() + " services matching filter:");
      for (RangerService service : services) {
        System.out.println("  id=" + service.getId() + ", name=" + service.getName() + ", type=" + service.getType());
      }
    } else {
      System.out.println("No services found for filter: " + serviceFilter);
    }

    // Get Service by name
    RangerService serviceByName = rangerClient.getService(RANGER_SERVICE);
    System.out.println("Get service by name: id=" + serviceByName.getId() + ", name=" + serviceByName.getName()
        + ", type=" + serviceByName.getType());

    // Get Service by ID
    RangerService serviceById = rangerClient.getService(serviceByName.getId());
    System.out.println("Get service by id: id=" + serviceById.getId() + ", name=" + serviceById.getName());
  }
}
