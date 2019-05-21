module Gush
  class Job
    attr_accessor :workflow_id, :incoming, :outgoing, :params,
      :finished_at, :failed_at, :started_at, :enqueued_at, :payloads, :klass, :queue
    attr_reader :id, :klass, :output_payload, :params, :error

    class << self
      attr_reader :sidekiq_retry_in_block
      def sidekiq_retry_in(&block)
        @sidekiq_retry_in_block = block
      end

      def sidekiq_options(options={})
        @sidekiq_options_hash = options
      end

      def sidekiq_options_hash
        @sidekiq_options_hash ||= {}
      end

      def gush_options(options={})
        @gush_options_hash = options
      end

      def gush_options_hash
        @gush_options_hash ||= {}
      end
    end

    def initialize(opts = {})
      options = opts.dup
      assign_variables(options)
    end

    def as_json
      {
        id: id,
        klass: klass.to_s,
        queue: queue,
        status: status,
        incoming: incoming,
        outgoing: outgoing,
        enqueued_at: enqueued_at,
        started_at: started_at,
        failed_at: failed_at,
        finished_at: finished_at,
        params: params,
        workflow_id: workflow_id,
        output_payload: output_payload,
        error: error
      }
    end

    def name
      @name ||= "#{klass}|#{id}"
    end

    def payload(clazz)
      payload = payloads.detect { |f| f[:class] == clazz.name }
      raise "Unable to find payload for #{clazz}, available: #{payloads.collect { |f| f[:class]}}" unless payload
      payload[:output]
    end

    def to_json(options = {})
      Gush::JSON.encode(as_json, options)
    end

    def self.from_hash(hash)
      hash[:klass].constantize.new(hash)
    end

    def output(data)
      @output_payload = data
    end

    def perform
    end

    def enqueue!
      @enqueued_at = current_timestamp
      @started_at = nil
      @finished_at = nil
      @failed_at = nil
      @error = nil
    end

    def start!
      @started_at = current_timestamp
      @failed_at = nil
      @error = nil
    end

    def finish!
      @finished_at = current_timestamp
    end

    def succeed!
      @failed_at = nil
      @error = nil
      self.finish!
    end

    def error!(error)
      @failed_at = current_timestamp
      @error = error
    end

    def fail!(error = nil)
      self.error! error
      self.finish!
    end

    def pending?
      enqueued_at.nil?
    end

    def enqueued?
      !pending?
    end

    def started?
      !started_at.nil?
    end

    def running?
      started? && !finished?
    end

    def retrying?
      running? && failed?
    end

    def finished?
      !finished_at.nil?
    end

    def remaining?
      !finished?
    end

    def succeeded?
      finished? && !failed?
    end

    def failed?
      !failed_at.nil?
    end

    def status
      if finished?
        return :succeeded if succeeded?
        return :failed if failed?
        raise StandardError, 'Unknown state'
      end

      if started?
        return :retrying if failed?
        return :running
      end

      return :enqueued if enqueued?

      :pending
    end

    def ready_to_start?
      !running? && !enqueued? && !finished? && !failed? && parents_succeeded?
    end

    def parents_succeeded?
      !incoming.any? do |name|
        !client.find_job(workflow_id, name).succeeded?
      end
    end

    def has_no_dependencies?
      incoming.empty?
    end

    private

    def client
      @client ||= Client.new
    end

    def current_timestamp
      Time.now.to_i
    end

    def assign_variables(opts)
      @id             = opts[:id]
      @incoming       = opts[:incoming] || []
      @outgoing       = opts[:outgoing] || []
      @failed_at      = opts[:failed_at]
      @finished_at    = opts[:finished_at]
      @started_at     = opts[:started_at]
      @enqueued_at    = opts[:enqueued_at]
      @params         = opts[:params] || {}
      @klass          = opts[:klass] || self.class
      @output_payload = opts[:output_payload]
      @workflow_id    = opts[:workflow_id]
      @queue          = opts[:queue]
      @error          = opts[:error]
    end
  end
end
