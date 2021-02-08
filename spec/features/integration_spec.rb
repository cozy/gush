require "spec_helper"
require "pry"

describe "Workflows" do
  context "when all jobs finish successfuly" do
    it "marks workflow as completed" do
      flow = TestWorkflow.create
      flow.start!
      Gush::Worker.drain

      flow = flow.reload
      expect(flow).to be_finished
      expect(flow).to_not be_failed
    end
  end

  it "runs the whole workflow in proper order" do
    flow = TestWorkflow.create
    flow.start!

    expect(Gush::Worker).to have_jobs(flow.id, jobs_with_id(%w[Prepare]))

    Gush::Worker.perform_one
    expect(Gush::Worker).to have_jobs(flow.id, jobs_with_id(%w[FetchFirstJob FetchSecondJob]))

    Gush::Worker.perform_one
    expect(Gush::Worker).to have_jobs(flow.id, jobs_with_id(%w[FetchSecondJob PersistFirstJob]))

    Gush::Worker.perform_one
    expect(Gush::Worker).to have_jobs(flow.id, jobs_with_id(%w[PersistFirstJob]))

    Gush::Worker.perform_one
    expect(Gush::Worker).to have_jobs(flow.id, jobs_with_id(%w[NormalizeJob]))

    Gush::Worker.perform_one
    expect(Gush::Worker.jobs).to be_empty
  end

  it "passes payloads down the workflow" do
    class UpcaseJob < Gush::Job
      def perform
        output params[:input].upcase
      end
    end

    class PrefixJob < Gush::Job
      def perform
        output params[:prefix].capitalize
      end
    end

    class PrependJob < Gush::Job
      def perform
        string = "#{payloads.find { |j| j[:class] == "PrefixJob"}[:output]}: #{payloads.find { |j| j[:class] == "UpcaseJob"}[:output]}"
        output string
      end
    end

    class PayloadWorkflow < Gush::Workflow
      def configure
        run UpcaseJob, params: {input: "some text"}
        run PrefixJob, params: {prefix: "a prefix"}
        run PrependJob, after: [UpcaseJob, PrefixJob]
      end
    end

    flow = PayloadWorkflow.create
    flow.start!

    Gush::Worker.perform_one
    expect(flow.reload.find_job("UpcaseJob").output_payload).to eq("SOME TEXT")

    Gush::Worker.perform_one
    expect(flow.reload.find_job("PrefixJob").output_payload).to eq("A prefix")

    Gush::Worker.perform_one
    expect(flow.reload.find_job("PrependJob").output_payload).to eq("A prefix: SOME TEXT")


  end

  it "passes payloads from workflow that runs multiple same class jobs with nameized payloads" do
    class RepetitiveJob < Gush::Job
      def perform
        output params[:input]
      end
    end

    class SummaryJob < Gush::Job
      def perform
        output payloads.map { |payload| payload[:output] }
      end
    end

    class PayloadWorkflow < Gush::Workflow
      def configure
        jobs = []
        jobs << run(RepetitiveJob, params: {input: "first"})
        jobs << run(RepetitiveJob, params: {input: "second"})
        jobs << run(RepetitiveJob, params: {input: "third"})
        run SummaryJob, after: jobs
      end
    end

    flow = PayloadWorkflow.create
    flow.start!

    flow = flow.reload
    repetitives = flow.jobs.select { |j| j.klass == RepetitiveJob.name }
    repetitives.size.times { Gush::Worker.perform_one }
    flow = flow.reload
    repetitives = flow.jobs.select { |j| j.klass == RepetitiveJob.name }
    repetitives.each do |job|
      expect(job.output_payload).to eq job.params[:input]
    end

    Gush::Worker.perform_one
    flow = flow.reload
    summary = flow.jobs.find { |j| j.klass == SummaryJob.name }
    expect(summary.output_payload).to eq(%w(first second third))
  end

  it "does not execute `configure` on each job for huge workflows" do
    INTERNAL_SPY = double("spy")
    INTERNAL_CONFIGURE_SPY = double("configure spy")
    expect(INTERNAL_SPY).to receive(:some_method).exactly(110).times

    # One time when persisting, second time when reloading in the spec
    expect(INTERNAL_CONFIGURE_SPY).to receive(:some_method).exactly(2).times

    class SimpleJob < Gush::Job
      def perform
        INTERNAL_SPY.some_method
      end
    end

    class GiganticWorkflow < Gush::Workflow
      def configure
        INTERNAL_CONFIGURE_SPY.some_method

        10.times do
          main = run(SimpleJob)
          10.times do
            run(SimpleJob, after: main)
          end
        end
      end
    end

    flow = GiganticWorkflow.create
    flow.start!

    110.times do
      Gush::Worker.perform_one
    end

    flow = flow.reload
    expect(flow).to be_finished
    expect(flow).to_not be_failed
  end

  it "executes job with multiple ancestors only once" do
    NO_DUPS_INTERNAL_SPY = double("spy")
    expect(NO_DUPS_INTERNAL_SPY).to receive(:some_method).exactly(1).times

    class FirstAncestor < Gush::Job
      def perform
      end
    end

    class SecondAncestor < Gush::Job
      def perform
      end
    end

    class FinalJob < Gush::Job
      def perform
        NO_DUPS_INTERNAL_SPY.some_method
      end
    end

    class NoDuplicatesWorkflow < Gush::Workflow
      def configure
        run FirstAncestor
        run SecondAncestor

        run FinalJob, after: [FirstAncestor, SecondAncestor]
      end
    end

    flow = NoDuplicatesWorkflow.create
    flow.start!

    3.times do
      Gush::Worker.perform_one
    end
    expect{Gush::Worker.perform_one}.to raise_error Sidekiq::EmptyQueueError

    flow = flow.reload
    expect(flow).to be_finished
    expect(flow).to_not be_failed
  end
end
