require 'sidekiq'
require 'redis-mutex'

module Gush
  class Worker
    include Sidekiq::Worker

    sidekiq_retries_exhausted do |args, ex|
      worker = self.new
      worker.setup_job *args['args']
      worker.fail! ex.message
    end

    def perform(workflow_id, job_id)
      setup_job(workflow_id, job_id)
      job.payloads = incoming_payloads
      start!

      begin
        job.perform
      rescue => error
        error! error.message
        raise error
      else
        succeed!
        enqueue_outgoing_jobs
      end
    end

    attr_reader :client, :workflow_id, :job

    def client
      @client ||= Gush::Client.new(Gush.configuration)
    end

    def setup_job(workflow_id, job_id)
      @workflow_id = workflow_id
      @job ||= client.find_job(workflow_id, job_id)

      self.class.sidekiq_retry_in &@job.class.sidekiq_retry_in_block
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

    def start!
      job.start!
      client.persist_job(workflow_id, job)
    end

    def succeed!
      job.succeed!
      client.persist_job(workflow_id, job)
    end

    def error!(error)
      job.error!(error)
      client.persist_job(workflow_id, job)
    end

    def fail!(error)
      job.fail!(error)
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
  end
end
