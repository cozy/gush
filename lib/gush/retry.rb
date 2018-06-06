module Gush
	class Job
		module Retry
			def retry_strategy(name, options = {})
				@retry = ActiveJob::Retry.new strategy: name, **options
			end
		end
	end
end
