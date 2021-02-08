require 'connection_pool'

module Gush
  class Client
    attr_reader :configuration

    def initialize(config = Gush.configuration)
      @configuration = config
    end

    def configure
      yield configuration
    end

    def create_workflow(name)
      begin
        name.constantize.create
      rescue NameError
        raise WorkflowNotFound.new("Workflow with given name doesn't exist")
      end
      flow
    end

    def start_workflow(workflow, job_names = [])
      workflow.mark_as_started
      persist_workflow(workflow)

      jobs = if job_names.empty?
               workflow.initial_jobs
             else
               job_names.map {|name| workflow.find_job(name) }
             end

      jobs.each do |job|
        enqueue_job(workflow.id, job)
      end
    end

    def stop_workflow(id)
      workflow = find_workflow(id)
      workflow.mark_as_stopped
      persist_workflow(workflow)
    end

    def next_free_job_id(workflow_id, job_klass)
      job_id = nil

      loop do
        job_id = SecureRandom.uuid
        available = connection_pool.with do |redis|
          !redis.hexists("gush.jobs.#{workflow_id}.#{job_klass}", job_id)
        end

        break if available
      end

      job_id
    end

    def next_free_workflow_id
      id = nil
      loop do
        id = SecureRandom.uuid
        available = connection_pool.with do |redis|
          !redis.exists?("gush.workflow.#{id}")
        end

        break if available
      end

      id
    end

    def all_workflows
      connection_pool.with do |redis|
        redis.scan_each(match: "gush.workflows.*").map do |key|
          id = key.sub("gush.workflows.", "")
          find_workflow(id)
        end
      end
    end

    def find_workflow(id)
      connection_pool.with do |redis|
        data = redis.get("gush.workflows.#{id}")

        unless data.nil?
          hash = Gush::JSON.decode(data, symbolize_keys: true)
          keys = redis.smembers("gush.classes.#{id}")

          nodes = keys.each_with_object([]) do |key, array|
            array.concat redis.hvals(key).map { |json| Gush::JSON.decode(json, symbolize_keys: true) }
          end

          workflow_from_hash(hash, nodes)
        else
          raise WorkflowNotFound.new("Workflow with given id doesn't exist")
        end
      end
    end

    def persist_workflow(workflow)
      connection_pool.with do |redis|
        redis.set(workflow.key, workflow.to_json)
      end

      workflow.jobs.each { |j| persist_job(j) }
      workflow.expire!
      workflow.mark_as_persisted

      true
    end

    def persist_job(job)
      connection_pool.with do |redis|
        redis.hset(job.key, job.id, job.to_json)
        redis.sadd("gush.classes.#{job.workflow_id}", job.key)
      end
    end

    def find_job_class(job_name)
      job_klass, _ = job_name.split('|')
      job_klass.constantize
    end

    def find_job(workflow_id, job_name)
      job_name_match = /(?<klass>.*)\|(?<identifier>.*)/.match(job_name)

      data = if job_name_match
               find_job_by_klass_and_id(workflow_id, job_name)
             else
               find_job_by_klass(workflow_id, job_name)
             end

      return nil if data.nil?

      data = Gush::JSON.decode(data, symbolize_keys: true)
      Gush::Job.from_hash(data)
    end

    def destroy_workflow(workflow)
      connection_pool.with do |redis|
        redis.del(workflow.key)
        redis.del("gush.classes.#{workflow.id}")
      end
      workflow.jobs.each { |j| destroy_job(j) }
    end

    def destroy_job(job)
      connection_pool.with do |redis|
        redis.del(job.key)
        redis.srem("gush.classes.#{job.workflow_id}", job.key)
      end
    end

    def expire_workflow(workflow, ttl=nil)
      ttl = ttl || configuration.ttl
      keys = [workflow.key, "gush.classes.#{workflow.id}"] + workflow.jobs.collect { |j| j.key }
      persist_or_expire *keys, ttl: ttl
    end

    def expire_job(job, ttl=nil)
      ttl = ttl || configuration.ttl
      persist_or_expire job.key, ttl: ttl
    end

    def enqueue_job(workflow_id, job)
      job.enqueue!
      persist_job(job)
      queue = job.queue || configuration.namespace
      options = { queue: queue }
      options.merge! job.class.gush_options_hash
      options.merge! job.class.sidekiq_options_hash
      Gush::Worker.set(options).perform_async(workflow_id, job.name)
    end

    private

    def persist_or_expire(*keys, ttl: nil)
      action = (ttl.nil? || ttl < 0) ? :persist : :expire
      connection_pool.with do |redis|
        if action == :persist
          keys.each { |k| redis.persist(k) }
        else
          keys.each { |k| redis.expire(k, ttl) }
        end
      end
    end

    def find_job_by_klass_and_id(workflow_id, job_name)
      job_klass, job_id = job_name.split('|')
      connection_pool.with do |redis|
        redis.hget("gush.jobs.#{workflow_id}.#{job_klass}", job_id)
      end
    end

    def find_job_by_klass(workflow_id, klass)
      _, result = connection_pool.with do |redis|
        redis.hscan("gush.jobs.#{workflow_id}.#{klass}", 0, count: 1)
      end

      return nil if result.empty?

      _, job = *result[0]

      job
    end

    def workflow_from_hash(hash, nodes = [])
      flow = hash[:klass].constantize.new(*hash[:arguments])
      flow.jobs = []
      flow.stopped = hash.fetch(:stopped, false)
      flow.id = hash[:id]

      flow.jobs = nodes.map do |node|
        Gush::Job.from_hash(node)
      end

      flow
    end

    def build_redis
      Redis.new(url: configuration.redis_url).tap do |instance|
        RedisClassy.redis = instance
      end
    end

    def connection_pool
      @connection_pool ||= ConnectionPool.new(size: configuration.concurrency, timeout: 1) { build_redis }
    end
  end
end
