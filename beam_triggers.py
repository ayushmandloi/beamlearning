from itertools import accumulate
from unittest import TestCase, main
from datetime import datetime
import apache_beam as beam
from apache_beam.transforms import window
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.trigger import AccumulationMode, Repeatedly, AfterProcessingTime


import collections


options = PipelineOptions()
options.view_as(StandardOptions).streaming = True

pipe = TestPipeline(options=options)

right_now = 0

# Data arrives in the interval of 100 seconds here 
# Window size = 100s
# trigger fires every 100s
# The code below shows how accumulate and discarding strategy of triggers works

op = (pipe
      | TestStream()
      .add_elements([("window_1", 20)], event_timestamp=right_now+20)

      .advance_processing_time(100)

      .add_elements([("window_1", 40)], event_timestamp=right_now+40)
      .add_elements([("window_2", 170)], event_timestamp=right_now + 170)

      .advance_processing_time(100)

      .add_elements([("window_1", 70)], event_timestamp=right_now+70)
      .add_elements([("window_1", 230)], event_timestamp=right_now+230)

      .advance_processing_time(100)

      .add_elements([("window_2", 150)], event_timestamp=right_now + 150)
      .add_elements([("window_3", 250)], event_timestamp=right_now + 250)

      .advance_processing_time(100)

      | beam.WindowInto(window.FixedWindows(100), 
      trigger=Repeatedly(AfterProcessingTime(100)) ,
      # accumulation_mode=AccumulationMode.ACCUMULATING),
      accumulation_mode=AccumulationMode.DISCARDING)
      | beam.GroupBy(lambda x: "default")
      | beam.Map(print)
      )

pipe.run().wait_until_finish()
