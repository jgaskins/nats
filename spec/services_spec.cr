require "./spec_helper"
require "wait_group"

require "../src/services"

nats = NATS::Client.new
  .on_error { |ex| pp ex }

describe NATS::Services do
  it "creates a service" do
    name = "test-service-#{UUID.v7}"
    svc = nats.services.add name,
      version: "0.1.0",
      description: "does a thing"

    svc.name.should eq name
  end

  it "creates endpoints" do
    name = "test-endpoint-#{UUID.v7}"
    svc = nats.services.add name,
      version: "0.1.0",
      description: "does a thing"
    root = svc.add_group(name)
    root.add_endpoint "upcase" do |request|
      nats.reply request, request.data_string.upcase
    end
    root.add_endpoint "downcase" do |request|
      nats.reply request, request.data_string.downcase
    end

    unless response = nats.request "#{name}.upcase", "hello"
      raise "Did not receive a response from the endpoint"
    end
    response.data_string.should eq "HELLO"
    unless response = nats.request "#{name}.downcase", "HELLO"
      raise "Did not receive a response from the endpoint"
    end
    response.data_string.should eq "hello"
  end

  it "cannot create an endpoint with an invalid name" do
    name = "test-endpoint-#{UUID.v7}"
    svc = nats.services.add name,
      version: "0.1.0",
      description: "does a thing"
    root = svc.add_group(name)
    expect_raises ArgumentError do
      root.add_endpoint "get.*" do |request|
        nats.reply request, request.data_string.upcase
      end
    end
  end

  it "responds to pings across all services" do
    name = "test-ping-#{UUID.v7}"
    svc = nats.services.add name,
      version: "0.1.0",
      description: "does a thing"

    nats.request("$SRV.PING", "").should_not be_nil
  end

  it "responds to pings at the service level" do
    name = "test-ping-service-#{UUID.v7}"
    svc = nats.services.add name,
      version: "0.1.0",
      description: "does a thing"

    nats.request("$SRV.PING.#{name}", "").should_not be_nil
  end

  it "responds to pings at the service id level" do
    name = "test-ping-service-id-#{UUID.v7}"
    svc = nats.services.add name,
      version: "0.1.0",
      description: "does a thing"

    nats.request("$SRV.PING.#{name}.#{svc.id}", "").should_not be_nil
  end

  it "responds with stats across all services" do
    name = "test-stats-#{UUID.v7}"
    svc = nats.services.add name,
      version: "0.1.0",
      description: "does a thing"

    nats.request("$SRV.STATS", "").should_not be_nil
  end

  it "responds with stats for the service" do
    name = "test-stats-service-#{UUID.v7}"
    svc = nats.services.add name,
      version: "0.1.0",
      description: "does a thing"
    group = svc.add_group name
    group.add_endpoint "foo" do |request|
      nats.reply request, "stuff"
    end
    nats.request "#{name}.foo", ""

    unless response = nats.request("$SRV.STATS.#{name}", "")
      raise "Did not receive a response from the endpoint"
    end
    stats = JSON.parse(response.data_string)

    stats["endpoints"][0]["num_requests"].should eq 1
  end

  it "responds with stats for the service with the given id" do
    name = "test-stats-service-id-#{UUID.v7}"
    svc = nats.services.add name,
      version: "0.1.0",
      description: "does a thing"
    group = svc.add_group name
    group.add_endpoint "foo" do |request|
      nats.reply request, "stuff"
    end
    nats.request "#{name}.foo", ""

    unless response = nats.request("$SRV.STATS.#{name}.#{svc.id}", "")
      raise "Did not receive a response from the endpoint"
    end
    stats = JSON.parse(response.data_string)

    stats["endpoints"][0]["num_requests"].should eq 1
  end

  it "responds with info for all services" do
    name = "test-info-service-#{UUID.v7}"
    svc = nats.services.add name,
      version: "0.1.0",
      description: "does a thing"

    nats.request("$SRV.INFO", "").should_not be_nil
  end

  it "responds with info for this specific service" do
    name = "test-info-service-id-#{UUID.v7}"
    svc = nats.services.add name,
      version: "0.1.0",
      description: "does a thing"

    unless response = nats.request("$SRV.INFO.#{name}", "")
      raise "Did not receive a response from the endpoint"
    end
    info = JSON.parse(response.data_string)

    info["type"].should eq "io.nats.micro.v1.info_response"
    info["name"].should eq name
    info["id"].should eq svc.id
    info["description"].should eq "does a thing"
    info["endpoints"].raw.should be_a Array(JSON::Any)
  end

  it "responds with info for this service with the specific id" do
    name = "test-info-service-id-#{UUID.v7}"
    svc = nats.services.add name,
      version: "0.1.0",
      description: "does a thing"

    unless response = nats.request("$SRV.INFO.#{name}.#{svc.id}", "")
      raise "Did not receive a response from the endpoint"
    end
    info = JSON.parse(response.data_string)

    info["type"].should eq "io.nats.micro.v1.info_response"
  end

  it "responds with Nats-Service-Error and Nats-Service-Error-Code if an exception is raised in the block" do
    name = "test-service-errors-#{UUID.v7}"
    svc = nats.services.add name,
      version: "0.1.0",
      description: "does a thing"
    group = svc.add_group name
    endpoint = group.add_endpoint "oops" do
      raise "hell"
    end

    unless response = nats.request("#{name}.oops", "")
      raise "Did not receive a response from the endpoint"
    end

    response.headers["Nats-Service-Error-Code"].should eq "500"
    response.headers["Nats-Service-Error"].should eq "hell"
    endpoint.total_errors.should eq 1
    endpoint.last_error.should eq "hell"
  end

  it "allows nested groups" do
    name = "test-nested-groups-#{UUID.v7}"
    svc = nats.services.add name,
      version: "0.1.0",
      description: "does a thing"

    # This also tests that we can pass a block to add_group, so that's neat
    svc.add_group name do |group|
      group.add_group "a" do |a|
        a.add_endpoint "stuff" do |msg|
          nats.reply msg, "yep"
        end
      end
    end

    nats.request("#{name}.a.stuff", "")
      .try(&.data_string)
      .should eq "yep"
  end

  it "allows concurrent requests to the same endpoint" do
    name = "test-concurrent-requests-#{UUID.v7}"
    svc = nats.services.add name,
      version: "0.1.0",
      description: "does a thing"
    subject = "#{name}.call"
    endpoint = svc.add_endpoint "lol", subject: subject, concurrency: 100 do |request, subscription|
      sleep 10.milliseconds
      nats.reply request, ""
    end

    start = Time.monotonic
    WaitGroup.wait do |wg|
      100.times do
        wg.spawn do
          unless response = nats.request subject
            raise "Did not receive a response from the endpoint"
          end
        end
      end
    end
    (Time.monotonic - start).should be_within 10.milliseconds, of: 10.milliseconds
  end
end
