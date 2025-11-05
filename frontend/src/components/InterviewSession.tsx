import { io, Socket } from 'socket.io-client';
import { useState, useEffect, useRef } from 'react';
import { Mic, MicOff, Send, CheckCircle, Loader2, Volume2, Clock } from 'lucide-react';

interface InterviewSessionProps {
  interviewId: string;
  candidateName: string;
  candidateEmail: string;
  apiBaseUrl: string;
}

export default function InterviewSession({
  interviewId,
  candidateName,
  candidateEmail,
  apiBaseUrl,
}: InterviewSessionProps) {
  const [responseId, setResponseId] = useState<string | null>(null);
  const [currentQuestion, setCurrentQuestion] = useState<string>('');
  const [questionNumber, setQuestionNumber] = useState(0);
  const [totalQuestions, setTotalQuestions] = useState(0);
  const [transcript, setTranscript] = useState('');
  const [isRecording, setIsRecording] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [interviewComplete, setInterviewComplete] = useState(false);
  const [finalAnalysis, setFinalAnalysis] = useState<any>(null);
  const [isPlayingAudio, setIsPlayingAudio] = useState(false);
  const [timeRemaining, setTimeRemaining] = useState<number | null>(null); // Time remaining in seconds

  const mediaRecorderRef = useRef<MediaRecorder | null>(null);
  const audioChunksRef = useRef<Blob[]>([]);
  const audioRef = useRef<HTMLAudioElement | null>(null);
  const socketRef = useRef<Socket | null>(null);
  const timerIntervalRef = useRef<NodeJS.Timeout | null>(null);

  useEffect(() => {
    let cancelled = false;
    const startSession = async () => {
      setLoading(true);
      try {
        const response = await fetch(`${apiBaseUrl}/api/interview/start-interview`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            interview_id: interviewId,
            candidate_name: candidateName,
            candidate_email: candidateEmail,
          }),
        });

        if (cancelled) return;

        if (!response.ok) {
          const errorData = await response.json().catch(() => ({ detail: 'Failed to start interview' }));
          throw new Error(errorData.detail || 'Failed to start interview');
        }

        const data = await response.json();
        if (cancelled) return;
        
        setResponseId(data.response_id);
        
        if (data.duration_minutes && data.duration_minutes > 0) {
          const totalSeconds = data.duration_minutes * 60;
          setTimeRemaining(totalSeconds);
        }

        if (data.session_id && data.session_token) {
          const socket = io(apiBaseUrl.replace('/api', ''), {
            transports: ['websocket'],
          });

          socket.on('connect', () => {
            socket.emit('start_interview', {
              session_id: data.session_id,
              response_id: data.response_id,
              session_token: data.session_token,
            });
          });

          socket.on('transcript_result', (data: { text: string }) => {
            if (!cancelled) {
              setTranscript((prev) => prev + ' ' + data.text);
            }
          });

          socketRef.current = socket;

          if (data.response_id && !cancelled) {
            fetchCurrentQuestion(data.response_id);
          }
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : 'An error occurred');
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    };

    startSession();
    
    // Cleanup on unmount
    return () => {
      cancelled = true;
      if (timerIntervalRef.current) {
        clearInterval(timerIntervalRef.current);
      }
      if (socketRef.current) {
        socketRef.current.disconnect();
      }
    };
  }, []);

  // Timer countdown effect
  useEffect(() => {
    if (timeRemaining === null || timeRemaining <= 0 || interviewComplete) {
      if (timerIntervalRef.current) {
        clearInterval(timerIntervalRef.current);
        timerIntervalRef.current = null;
      }
      // Auto-end interview when time runs out
      if (timeRemaining === 0 && !interviewComplete && responseId) {
        endInterviewManually();
      }
      return;
    }

    // Start timer countdown
    timerIntervalRef.current = setInterval(() => {
      setTimeRemaining((prev) => {
        if (prev === null || prev <= 1) {
          return 0;
        }
        return prev - 1;
      });
    }, 1000);

    return () => {
      if (timerIntervalRef.current) {
        clearInterval(timerIntervalRef.current);
      }
    };
  }, [timeRemaining, interviewComplete, responseId]);


  const fetchCurrentQuestion = async (resId: string, abortSignal?: AbortSignal) => {
    try {
      const response = await fetch(`${apiBaseUrl}/api/interview/get-current-question`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          response_id: resId
        }),
        signal: abortSignal
      });

      if (abortSignal?.aborted) return;

      if (!response.ok) {
        throw new Error('Failed to fetch question');
      }

      const data = await response.json();
      
      if (abortSignal?.aborted) return;
      
      console.log('[DEBUG] Question response:', { 
        has_tts: !!data.tts_audio_base64, 
        tts_size: data.tts_audio_base64 ? data.tts_audio_base64.length : 0,
        question: data.current_question 
      });

      if (data.complete === true || data.interview_complete === true) {
        setInterviewComplete(true);
      } else {
        const questionText = typeof data.current_question === 'string'
          ? data.current_question
          : data.current_question?.question || data.current_question?.text || '';

        setCurrentQuestion(questionText);
        setQuestionNumber(data.question_number || 0);
        setTotalQuestions(data.total_questions || 0);

        if (data.tts_audio_base64) {
          console.log('[DEBUG] Playing TTS audio, base64 length:', data.tts_audio_base64.length);
          playTTSAudio(data.tts_audio_base64);
        } else {
          console.warn('[WARN] No TTS audio in response');
        }
      }
    } catch (err) {
      if (err instanceof Error && err.name === 'AbortError') return;
      console.error('[ERROR] Failed to fetch question:', err);
      setError(err instanceof Error ? err.message : 'An error occurred');
    }
  };

  const playTTSAudio = (base64Audio: string) => {
    try {
      console.log('[DEBUG] Decoding TTS audio, base64 length:', base64Audio.length);
      const audioData = atob(base64Audio);
      console.log('[DEBUG] Decoded audio data length:', audioData.length);
      
      const arrayBuffer = new ArrayBuffer(audioData.length);
      const view = new Uint8Array(arrayBuffer);
      for (let i = 0; i < audioData.length; i++) {
        view[i] = audioData.charCodeAt(i);
      }

      const blob = new Blob([arrayBuffer], { type: 'audio/mpeg' });
      const audioUrl = URL.createObjectURL(blob);
      console.log('[DEBUG] Created audio blob URL:', audioUrl);

      if (audioRef.current) {
        audioRef.current.pause();
        audioRef.current = null;
      }

      const audio = new Audio(audioUrl);
      audioRef.current = audio;

      audio.onplay = () => {
        console.log('[DEBUG] Audio started playing');
        setIsPlayingAudio(true);
      };
      audio.onended = () => {
        console.log('[DEBUG] Audio finished playing');
        setIsPlayingAudio(false);
        URL.revokeObjectURL(audioUrl);
      };
      audio.onerror = (e) => {
        console.error('[ERROR] Audio playback error:', e);
        setIsPlayingAudio(false);
        URL.revokeObjectURL(audioUrl);
      };
      audio.onloadstart = () => console.log('[DEBUG] Audio loading started');
      audio.oncanplay = () => console.log('[DEBUG] Audio can play');

      console.log('[DEBUG] Attempting to play audio...');
      audio.play().catch(err => {
        console.error('[ERROR] Error playing TTS audio:', err);
        setIsPlayingAudio(false);
      });
    } catch (err) {
      console.error('[ERROR] Error processing TTS audio:', err);
    }
  };

  const replayQuestion = () => {
    if (audioRef.current) {
      audioRef.current.currentTime = 0;
      audioRef.current.play();
    }
  };

  const startRecording = async () => {
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
  
      const mediaRecorder = new MediaRecorder(stream, {
        mimeType: 'audio/webm;codecs=opus',
      });
  
      mediaRecorderRef.current = mediaRecorder;
      audioChunksRef.current = [];
  
      mediaRecorder.ondataavailable = async (event) => {
        if (event.data.size > 0) {
          audioChunksRef.current.push(event.data);
        }
      };
  
      mediaRecorder.onstop = async () => {
        stream.getTracks().forEach((track) => track.stop());
  
        // Merge all received chunks
        if (audioChunksRef.current.length > 0) {
          const merged = new Blob(audioChunksRef.current, {
            type: 'audio/webm;codecs=opus',
          });
  
          // Optional: Debug
          console.log('[DEBUG] Merged blob', merged);
  
          // Send the merged blob as ArrayBuffer via socket
          const buf = await merged.arrayBuffer();
          if (socketRef.current) socketRef.current.emit('send_audio_chunk', buf);
  
          // Optionally: Clear chunks or keep for debugging
          audioChunksRef.current = [];
        }
      };
  
      mediaRecorder.start(1000); // Granularity doesn't matter, we collect all
      setIsRecording(true);
      setTranscript('Recording...');
    } catch (err) {
      setError('Could not access microphone. Please check permissions.');
      console.error('Error accessing microphone:', err);
    }
  };

  const stopRecording = () => {
    if (mediaRecorderRef.current && mediaRecorderRef.current.state !== 'inactive') {
      mediaRecorderRef.current.stop();
      setIsRecording(false);
    }
  };

  // Removed transcribeAudio(): live transcript arrives via socket 'transcript_result'

  const toggleRecording = () => {
    if (isRecording) {
      stopRecording();
    } else {
      startRecording();
    }
  };

  const submitAnswer = async () => {
    if (!responseId || !transcript.trim() || transcript === 'Recording...') {
      setError('Please record an answer first');
      return;
    }

    setLoading(true);
    try {
      const response = await fetch(`${apiBaseUrl}/api/interview/submit-answer`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          response_id: responseId,
          question: currentQuestion?.question || '',
          transcript: transcript,
        }),
      });

      if (!response.ok) {
        throw new Error('Failed to submit answer');
      }

      const data = await response.json();

      if (data.complete || data.interview_completed) {
        setInterviewComplete(true);
        if (data.final_analysis) {
        setFinalAnalysis(data.final_analysis);
        }
        // Update question counts when interview completes naturally
        if (data.question_number !== undefined) {
          setQuestionNumber(data.question_number);
        }
        if (data.total_questions !== undefined) {
          setTotalQuestions(data.total_questions);
        }
      } else {
        setTranscript('');
        audioChunksRef.current = [];
        await fetchCurrentQuestion(responseId);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  };

  // Format time remaining as MM:SS
  const formatTime = (seconds: number): string => {
    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    return `${mins.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
  };

  const endInterviewManually = async () => {
    if (!responseId || interviewComplete) return;

    // Clear timer
    if (timerIntervalRef.current) {
      clearInterval(timerIntervalRef.current);
      timerIntervalRef.current = null;
    }

    setLoading(true);
    try {
      const response = await fetch(`${apiBaseUrl}/api/interview/end-interview`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          response_id: responseId
        }),
      });

      if (!response.ok) {
        throw new Error('Failed to end interview');
      }

      const endData = await response.json();
      setInterviewComplete(true);
      
      // Update question counts from backend response
      if (endData.questions_answered !== undefined) {
        setQuestionNumber(endData.questions_answered);
      }
      if (endData.total_questions !== undefined) {
        setTotalQuestions(endData.total_questions);
      }
      
      // Fetch final analysis from response detail if needed
      if (responseId) {
        try {
          const detailRes = await fetch(`${apiBaseUrl}/api/interview/get-response`, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
            },
            body: JSON.stringify({
              response_id: responseId
            }),
          });
          if (detailRes.ok) {
            const detailData = await detailRes.json();
            setFinalAnalysis(detailData.general_summary);
          }
        } catch (e) {
          console.error('Failed to fetch final analysis', e);
        }
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  };

  if (interviewComplete) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-emerald-50 to-teal-50 flex items-center justify-center p-6">
        <div className="bg-white rounded-2xl shadow-xl max-w-3xl w-full p-8">
          <div className="flex items-center gap-3 mb-8">
            <div className="bg-green-600 p-3 rounded-xl">
              <CheckCircle className="w-8 h-8 text-white" />
            </div>
            <div>
              <h1 className="text-3xl font-bold text-gray-900">Interview Complete!</h1>
              <p className="text-gray-600">Thank you for participating</p>
            </div>
          </div>

          <div className="bg-green-50 border border-green-200 rounded-lg p-6 mb-6">
            <h2 className="text-xl font-semibold text-gray-900 mb-2">Summary</h2>
            <p className="text-gray-700">
              {candidateName}, your interview has been completed successfully.
            </p>
            {totalQuestions > 0 && (
              <p className="text-gray-600 mt-2">
                Questions answered: {questionNumber}/{totalQuestions}
              </p>
            )}
          </div>

          {finalAnalysis && (
            <div className="bg-blue-50 border border-blue-200 rounded-lg p-6">
              <h2 className="text-xl font-semibold text-gray-900 mb-4">Analysis</h2>
              {finalAnalysis.overall_score && (
                <div className="mb-4">
                  <p className="text-gray-700 font-semibold">
                    Overall Score: {finalAnalysis.overall_score}/100
                  </p>
                </div>
              )}
              {finalAnalysis.recommendations && finalAnalysis.recommendations.length > 0 && (
                <div>
                  <p className="text-gray-700 font-semibold mb-2">Recommendations:</p>
                  <ul className="list-disc list-inside space-y-1">
                    {finalAnalysis.recommendations.map((rec: string, idx: number) => (
                      <li key={idx} className="text-gray-600">{rec}</li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-orange-50 to-amber-50 flex items-center justify-center p-6">
      <div className="bg-white rounded-2xl shadow-xl max-w-4xl w-full p-8">
        <div className="mb-8">
          <div className="flex items-center justify-between mb-4">
            <h1 className="text-3xl font-bold text-gray-900">Interview Session</h1>
            <div className="flex items-center gap-4">
              {/* Timer Display */}
              {timeRemaining !== null && timeRemaining >= 0 && (
                <div className={`flex items-center gap-2 px-4 py-2 rounded-full font-semibold ${
                  timeRemaining <= 300 // Less than 5 minutes
                    ? 'bg-red-100 text-red-800 animate-pulse'
                    : timeRemaining <= 600 // Less than 10 minutes
                    ? 'bg-orange-100 text-orange-800'
                    : 'bg-blue-100 text-blue-800'
                }`}>
                  <Clock className="w-5 h-5" />
                  <span>{formatTime(timeRemaining)}</span>
                </div>
              )}
              {totalQuestions > 0 && (
                <span className="bg-blue-100 text-blue-800 px-4 py-2 rounded-full font-semibold">
                  Question {questionNumber}/{totalQuestions}
                </span>
              )}
            </div>
          </div>
          <p className="text-gray-600">Candidate: {candidateName}</p>
        </div>

        {error && (
          <div className="mb-6 p-4 bg-red-50 border border-red-200 rounded-lg text-red-700">
            {error}
          </div>
        )}

        <div className="mb-8 p-6 bg-gradient-to-r from-blue-50 to-blue-100 rounded-xl border-l-4 border-blue-600">
          <div className="flex items-start justify-between">
            <div className="flex-1">
              <h2 className="text-sm font-semibold text-gray-600 mb-2">CURRENT QUESTION</h2>
              <p className="text-xl text-gray-900 font-medium leading-relaxed">
                {currentQuestion || 'Loading question...'}
              </p>
            </div>
            {audioRef.current && (
              <button
                onClick={replayQuestion}
                disabled={isPlayingAudio}
                className="ml-4 p-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition disabled:opacity-50"
                title="Replay question"
              >
                <Volume2 className={`w-5 h-5 ${isPlayingAudio ? 'animate-pulse' : ''}`} />
              </button>
            )}
          </div>
          {isPlayingAudio && (
            <p className="text-sm text-blue-600 mt-2 animate-pulse">Playing question audio...</p>
          )}
        </div>

        <div className="mb-6">
          <label className="block text-sm font-semibold text-gray-700 mb-3">
            Your Answer {isRecording && <span className="text-red-600 animate-pulse">(Recording...)</span>}
          </label>
          <div className="relative">
            <textarea
              value={transcript}
              onChange={(e) => setTranscript(e.target.value)}
              rows={8}
              className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-orange-500 focus:border-transparent transition resize-none"
              placeholder="Click the microphone to start recording your answer or type here..."
              disabled={isRecording}
            />
          </div>
          {isRecording && (
            <p className="mt-2 text-sm text-gray-600">
              Recording in progress... Your voice will be transcribed automatically when you stop.
            </p>
          )}
        </div>

        <div className="flex gap-4">
          <button
            onClick={toggleRecording}
            disabled={loading}
            className={`flex-1 py-4 rounded-lg font-semibold transition shadow-lg hover:shadow-xl flex items-center justify-center gap-2 ${
              isRecording
                ? 'bg-red-600 hover:bg-red-700 text-white animate-pulse'
                : 'bg-orange-600 hover:bg-orange-700 text-white'
            } disabled:opacity-50 disabled:cursor-not-allowed`}
          >
            {isRecording ? (
              <>
                <MicOff className="w-5 h-5" />
                Stop Recording
              </>
            ) : (
              <>
                <Mic className="w-5 h-5" />
                Start Recording
              </>
            )}
          </button>

          <button
            onClick={submitAnswer}
            disabled={loading || !transcript.trim() || transcript === 'Recording...'}
            className="flex-1 bg-green-600 text-white py-4 rounded-lg font-semibold hover:bg-green-700 transition disabled:opacity-50 disabled:cursor-not-allowed shadow-lg hover:shadow-xl flex items-center justify-center gap-2"
          >
            {loading ? (
              <>
                <Loader2 className="w-5 h-5 animate-spin" />
                Submitting...
              </>
            ) : (
              <>
                <Send className="w-5 h-5" />
                Submit Answer
              </>
            )}
          </button>

          <button
            onClick={endInterviewManually}
            disabled={loading}
            className="px-6 bg-gray-600 text-white py-4 rounded-lg font-semibold hover:bg-gray-700 transition disabled:opacity-50 disabled:cursor-not-allowed shadow-lg hover:shadow-xl"
          >
            End Interview
          </button>
        </div>

        <div className="mt-4 p-4 bg-blue-50 border border-blue-200 rounded-lg">
          <h3 className="text-sm font-semibold text-gray-700 mb-2">How it works:</h3>
          <ul className="text-sm text-gray-600 space-y-1">
            <li>• The question will be read aloud automatically using text-to-speech</li>
            <li>• Click "Start Recording" to begin recording your answer</li>
            <li>• Speak clearly into your microphone</li>
            <li>• Click "Stop Recording" when finished</li>
            <li>• Your speech will be transcribed using backend STT service</li>
            <li>• Review the transcript and click "Submit Answer" to continue</li>
            <li>• Click the speaker icon to replay the question</li>
          </ul>
        </div>
      </div>
    </div>
  );
}
