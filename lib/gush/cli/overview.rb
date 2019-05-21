module Gush
  class CLI
    class Overview
      def initialize(workflow)
        @workflow = workflow
      end

      def table
        Terminal::Table.new(rows: rows)
      end

      STATUS = {
        stopped: {
          color: :light_black,
          label: 'Stopped',
          symbol: '⏸️'
        },
        pending: {
          color: :cyan,
          label: 'Pending',
          symbol: '💤'
        },
        enqueued: {
          color: :blue,
          label: 'Enqueued',
          symbol: '🕓'
        },
        running: {
          color: :magenta,
          label: 'Running',
          symbol: '⚙️'
        },
        retrying: {
          color: :yellow,
          label: 'Retrying',
          symbol: '🔁'
        },
        failed: {
          color: :red,
          label: 'Failed',
          symbol: '❌'
        },
        succeeded: {
          color: :green,
          label: 'Succeeded',
          symbol: '✅'
        }
      }.freeze

      def status(status)
        status = @workflow.status
        status = STATUS[status]
        status[:label].colorize status[:color]
      end

      def jobs_list(jobs)
        "\nJobs list:\n".tap do |output|
          jobs_by_type(jobs).each do |job|
            output << job_to_list_element(job)
          end
        end
      end

      def rows
        [].tap do |rows|
          columns.each_pair do |name, value|
            rows << [{alignment: :center, value: name}, value]
            rows << :separator if name != "Status"
          end
        end
      end

      def columns
        status = (STATUS.keys - %i[stopped]).collect do |status|
          count = jobs_count status
          next nil unless count > 0
          status = STATUS[status]
          color = status[:color]
          label = status[:label].colorize color
          count = count.to_s.colorize color
          ["#{label} jobs", count]
        end.compact.to_h
        {
                "ID" => @workflow.id,
                "Name" => @workflow.class.to_s,
                "Jobs" => @workflow.jobs.count,
        }.merge(status)
          .merge({
          "Remaining jobs" => self.remaining_jobs_count,
          "Started at" => self.time(@workflow.started_at),
          "Finished at" => self.time(@workflow.finished_at),
          "Status" => self.status(@workflow.status)
        })
      end

      def running_status
        finished = self.succeeded_jobs_count.to_i
        status = self.status :running
        status += "\n#{finished}/#{total_jobs_count} [#{(finished*100)/total_jobs_count}%]"
        status
      end

      def time(time)
        Time.at time if time
      end

      def failed_status
        status = self.status :failed
        status += "\n#{self.failed_job} failed"
        status
      end

      def job_to_list_element(job)
        status = STATUS[job.status]
        color = status[:color]
        symbol = status[:symbol].colorize color
        name = job.name.colorize color
        "  #{symbol} #{name}\n"
      end

      def jobs_by_type(type)
        return sorted_jobs if type == :all
        jobs.select{|j| j.public_send("#{type}?") }
      end

      def sorted_jobs
        @workflow.jobs.sort_by { |j| STATUS.keys.reverse.index j.status }
      end

      def failed_job
        @workflow.jobs.find(&:failed?).name
      end

      def total_jobs_count
        @workflow.jobs.count
      end

      def jobs_count(status)
        @workflow.jobs.count &:"#{status}?"
      end

      def remaining_jobs_count
        @workflow.jobs.count(&:remaining?).to_s
      end
    end
  end
end
