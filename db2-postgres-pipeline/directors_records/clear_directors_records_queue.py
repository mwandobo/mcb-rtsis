#!/usr/bin/env python3
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from directors_records.directors_records_streaming_pipeline import DirectorsRecordsStreamingPipeline
if __name__ == '__main__':
 p=DirectorsRecordsStreamingPipeline(); p.setup_queue(); c,ch=p.rabbitmq_connection()
 try:
  for q in (p.queue_name,p.dead_letter_queue): ch.queue_purge(queue=q)
 finally: c.close()
