require 'active_job'
require 'redis-mutex'

module Gush
  class Worker < ::ActiveJob::Base
    def perform(workflow_id, job_id)
      setup_job(workflow_id, job_id)

      job.payloads = incoming_payloads

      error = nil
      mark_as_started

      begin
        job.perform
      rescue StandardError => error
        unless internal_retry error
          mark_as_failed error.message
          raise error
        end
      else
        mark_as_finished
        enqueue_outgoing_jobs
      end
    end

    def serialize
      super.merge 'retry_attempt' => retry_attempt
    end

    def deserialize(job_data)
      super job_data
      @retry_attempt = job_data.fetch 'retry_attempt', 1
    end

    private

    def retry_attempt
      @retry_attempt ||= 1
    end

    attr_reader :client, :workflow_id, :job

    def client
      @client ||= Gush::Client.new(Gush.configuration)
    end

    def setup_job(workflow_id, job_id)
      @workflow_id = workflow_id
      @job ||= client.find_job(workflow_id, job_id)
      @retry = @job.class.instance_variable_get :@retry
    end

    def incoming_payloads
      job.incoming.map do |job_name|
        job = client.find_job(workflow_id, job_name)
        {
          id: job.name,
          class: job.klass.to_s,
          output: job.output_payload
        }
      end
    end

    def mark_as_finished
      job.finish!
      client.persist_job(workflow_id, job)
    end

    def mark_as_failed(error)
      job.fail!(error)
      client.persist_job(workflow_id, job)
    end

    def mark_as_started
      job.start!
      client.persist_job(workflow_id, job)
    end

    def elapsed(start)
      (Time.now - start).to_f.round(3)
    end

    def enqueue_outgoing_jobs
      job.outgoing.each do |job_name|
        RedisMutex.with_lock("gush_enqueue_outgoing_jobs_#{workflow_id}-#{job_name}", sleep: 0.3, block: 2) do
          out = client.find_job(workflow_id, job_name)

          if out.ready_to_start?
            client.enqueue_job(workflow_id, out)
          end
        end
      end
    end

    def internal_retry(exception)
      return false unless @retry

      should_retry = @retry.should_retry? @retry_attempt, exception
      return false unless should_retry

      this_delay = @retry.retry_delay @retry_attempt, exception
      cb         = @retry.retry_callback

      cb = cb && instance_exec(exception, this_delay, &cb)
      return false if cb == :halt

      # TODO: This breaks DelayedJob and Resque for some weird ActiveSupport reason
      # logger.info("Retrying (attempt #{retry_attempt + 1}, waiting #{this_delay}s
      @retry_attempt += 1
      retry_job wait: this_delay
    end
  end
end
