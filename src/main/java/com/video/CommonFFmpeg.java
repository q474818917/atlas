package com.video;

import java.io.IOException;

import net.bramp.ffmpeg.FFmpeg;
import net.bramp.ffmpeg.FFmpegExecutor;
import net.bramp.ffmpeg.FFprobe;
import net.bramp.ffmpeg.builder.FFmpegBuilder;

public abstract class CommonFFmpeg implements FFmpegProcessor {
	
	private static FFmpegExecutor executor; 
	
	public CommonFFmpeg() {
		try {
			FFmpeg ffmpeg = new FFmpeg();
			FFprobe ffprobe = new FFprobe();
			executor = new FFmpegExecutor(ffmpeg, ffprobe);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	abstract FFmpegBuilder builder();
	
	@Override
	public void execute() {
		FFmpegBuilder ffmpegBuilder = builder();
		executor.createJob(ffmpegBuilder).run();
	}
}
