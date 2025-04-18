package com.privacera.example;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerSecurityZone;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;

public class RangerSecurityZoneDemo {

  private static final String RANGER_SERVICE = "privacera_s3";
  private static final String RANGER_SERVICE_TYPE = "s3";


  private static final String RANGER_ADMIN_URL = "https://ranger"; // Replace with your Ranger Admin URL. For Privacera Cloud : API Keys --> API Key Info --> Ranger Admin URL
  private static final String SECURITY_ZONE_API_ENDPOINT = "/service/public/v2/api/zones"; // Endpoint for Security Zones
  private static final String RANGER_ADMIN_USERNAME = "username"; // Replace with your Ranger username
  private static final String RANGER_ADMIN_PASSWORD = "Password#12"; // Replace with your Ranger password
  private static final String ACCOUNT_ID = "123839390303" ; // Replace with your Account ID
  
  public static void main(String[] args) throws RangerServiceException {



    Gson gson = new GsonBuilder()
        .registerTypeAdapter(Date.class, new JsonDeserializer<Date>() {
          @Override
          public Date deserialize(JsonElement json, java.lang.reflect.Type typeOfT, JsonDeserializationContext context) {
            return new Date(json.getAsJsonPrimitive().getAsLong());
          }
        })
        .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
        .create();

    HttpClient client = HttpClient.newHttpClient();

    // Create Ranger Security zone resource
    HashMap<String, List<String>> resourceMap = new HashMap<>();
    resourceMap.put("bucketname", Arrays.asList("bucket-133"));
    resourceMap.put("objectpath", Arrays.asList("*"));


    // Create Ranger Security Zone service
    RangerSecurityZone.RangerSecurityZoneService rangerSecurityZoneService = new RangerSecurityZone.RangerSecurityZoneService();
    rangerSecurityZoneService.setResources(Arrays.asList(resourceMap));

    Map<String, RangerSecurityZone.RangerSecurityZoneService> serviceMap = new HashMap<>();
    serviceMap.put("privacera_s3", rangerSecurityZoneService);


    RangerSecurityZone rangerSecurityZone = new RangerSecurityZone();
    rangerSecurityZone.setName("SecurityZone-1");
    rangerSecurityZone.setServices(serviceMap);
    rangerSecurityZone.setAdminUsers(Collections.singletonList("asit.vadhavkar"));
    rangerSecurityZone.setAuditUsers(Collections.singletonList("asit.vadhavkar"));

    String requestBody = gson.toJson(rangerSecurityZone);

    String uriString = RANGER_ADMIN_URL + SECURITY_ZONE_API_ENDPOINT + "?accountId=" + URLEncoder.encode(ACCOUNT_ID, StandardCharsets.UTF_8);

    try {


      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(uriString))
          .header("Content-Type", "application/json")
          .header("Authorization", getBasicAuthHeader(RANGER_ADMIN_USERNAME, RANGER_ADMIN_PASSWORD)) // Use the helper
          .POST(HttpRequest.BodyPublishers.ofString(requestBody))
          .build();

      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() >= 200 && response.statusCode() < 300) {
        System.out.println("Security Zone created successfully!");
        // Parse the JSON response into a RangerSecurityZone object
        RangerSecurityZone createdZone = gson.fromJson(response.body(), RangerSecurityZone.class);
        System.out.println("Created Security Zone Name: " + createdZone.getName());

        // Update Security zone - Add hive resource

        HashMap<String, List<String>> hiveResourceMap = new HashMap<>();
        hiveResourceMap.put("database", Arrays.asList("sales"));
        hiveResourceMap.put("table", Arrays.asList("*"));
        hiveResourceMap.put("column", Arrays.asList("*"));

        RangerSecurityZone.RangerSecurityZoneService rangerSecurityZoneHiveService = new RangerSecurityZone.RangerSecurityZoneService();
        rangerSecurityZoneHiveService.setResources(Arrays.asList(hiveResourceMap));

        createdZone.getServices().put("privacera_hive", rangerSecurityZoneHiveService);
        createdZone.setName("SecurityZone-1 - v2");
        String updatePayload = gson.toJson(createdZone);
        String updateURIString = RANGER_ADMIN_URL + SECURITY_ZONE_API_ENDPOINT +"/" + createdZone.getId() + "?accountId=" + URLEncoder.encode(ACCOUNT_ID, StandardCharsets.UTF_8);
        request = HttpRequest.newBuilder()
            .uri(URI.create(updateURIString))
            .header("Content-Type", "application/json")
            .header("Authorization", getBasicAuthHeader(RANGER_ADMIN_USERNAME, RANGER_ADMIN_PASSWORD)) // Use the helper
            .PUT(HttpRequest.BodyPublishers.ofString(updatePayload))
            .build();
        HttpResponse<String> updateResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (updateResponse.statusCode() >= 200 && updateResponse.statusCode() < 300) {
          System.out.println("Security Zone created successfully!");
          // Parse the JSON response into a RangerSecurityZone object
          RangerSecurityZone updatedZone = gson.fromJson(updateResponse.body(), RangerSecurityZone.class);
          System.out.println("Updated Security Zone Version : " + updatedZone.getName());
        }

      } else {
        System.err.println("Failed to create security zone.  Check the response for details.");
      }
    } catch (Exception ex) {
      System.out.println("Failed to create SZ" + ex.getMessage());
      ex.printStackTrace();
    }

    // Fetch all the Security Zones

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(uriString))
        .header("Authorization", getBasicAuthHeader(RANGER_ADMIN_USERNAME, RANGER_ADMIN_PASSWORD)) // Use the helper
        .GET()
        .build();

    try {
      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
      System.out.println("Status Code: " + response.statusCode());

      Type type = new TypeToken<List<RangerSecurityZone>>(){}.getType();
      List<RangerSecurityZone> securityZones = gson.fromJson(response.body(), type);

      System.out.println("Total Security Zones :" + securityZones.size());
      securityZones.forEach(securityZone -> {
        System.out.println("Security Zone ID: " + securityZone.getId() + " Security zone " + securityZone.getName());
      });

    } catch (IOException e) {
      System.err.println("Error during HTTP request: " + e.getMessage());
      e.printStackTrace();
    } catch (InterruptedException e) {
      System.err.println("Request interrupted: " + e.getMessage());
      Thread.currentThread().interrupt();
    }

  }


  // Helper method to generate the Basic Authentication header
  private static String getBasicAuthHeader(String username, String password) {
    return "Basic " + java.util.Base64.getEncoder().encodeToString((username + ":" + password).getBytes());
  }

}

