import { useState, useEffect } from 'react';
import { X } from 'lucide-react';

interface Question {
  id?: string;
  question: string;
  depth_level: 'low' | 'medium' | 'high';
}

interface EditQuestionsModalProps {
  interviewId: string;
  autoGenerate: boolean;
  mode: 'predefined' | 'dynamic';
  questionCount: number;
  apiBaseUrl: string;
  onSave: () => void;
  onClose: () => void;
}

export default function EditQuestionsModal({
  interviewId,
  autoGenerate,
  mode,
  questionCount,
  apiBaseUrl,
  onSave,
  onClose,
}: EditQuestionsModalProps) {
  const [questions, setQuestions] = useState<Question[]>([]);
  const [description, setDescription] = useState('');
  const [loading, setLoading] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    // First, try to load existing questions from the interview
    loadExistingQuestions();
  }, []);

  const loadExistingQuestions = async () => {
    let existingQuestions = false;
    try {
      const res = await fetch(`${apiBaseUrl}/api/interview/get-interview`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          interview_id: interviewId
        }),
      });
      if (res.ok) {
        const data = await res.json();
        
        // Load existing description if available
        if (data.description) {
          setDescription(data.description);
        }
        
        if (data.questions && data.questions.length > 0) {
          // Load existing questions
          const formatted = data.questions.map((q: any) => ({
            id: q.id,
            question: q.question || q.text || '',
            depth_level: (q.difficulty || q.depth_level || 'medium') as 'low' | 'medium' | 'high',
          }));
          setQuestions(formatted);
          existingQuestions = true;
          return;
        }
      }
    } catch (e) {
      console.error('Failed to load existing questions:', e);
    }

    // For dynamic mode, don't load/generate questions - they're created during interview
    // Description should already be generated at interview creation time
    if (mode === 'dynamic') {
      setQuestions([]);
      return; // Description should already be loaded from existing interview
    }

    // For predefined mode: if questions already exist (from auto-generation at creation), use them
    // Otherwise, generate or initialize based on autoGenerate flag
    if (autoGenerate && !existingQuestions) {
      // Only generate if questions don't already exist (weren't auto-generated at creation)
      generateQuestions();
    } else if (!autoGenerate) {
      // Manual mode: initialize empty questions
      setQuestions(Array(questionCount).fill(null).map(() => ({ question: '', depth_level: 'medium' as const })));
    }
  };

  const generateQuestions = async () => {
    setGenerating(true);
    setError('');
    try {
      const res = await fetch(`${apiBaseUrl}/api/interview/generate-questions`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 
          interview_id: interviewId,
          question_count: questionCount 
        }),
      });
      
      if (!res.ok) {
        const errorText = await res.text();
        throw new Error(`HTTP ${res.status}: ${errorText}`);
      }
      
      const data = await res.json();
      if (data.ok && data.questions) {
        const formatted = data.questions.map((q: any) => ({
          id: q.id, // Preserve ID
          question: q.question || q.text || '',
          depth_level: (q.difficulty || q.depth_level || 'medium') as 'low' | 'medium' | 'high',
        }));
        setQuestions(formatted);
        
        // Update description if it was auto-generated
        if (data.description && !description) {
          setDescription(data.description);
        }
      } else {
        setError('No questions generated. Please try again.');
      }
    } catch (e: any) {
      setError(e.message || 'Failed to generate questions');
      console.error('Generate questions error:', e);
    } finally {
      setGenerating(false);
    }
  };

  const handleQuestionChange = (idx: number, val: string) => {
    setQuestions(prev => prev.map((q, i) => i === idx ? { ...q, question: val } : q));
  };

  const handleDepthChange = (idx: number, val: 'low' | 'medium' | 'high') => {
    setQuestions(prev => prev.map((q, i) => i === idx ? { ...q, depth_level: val } : q));
  };

  const handleDelete = (idx: number) => {
    setQuestions(prev => prev.filter((_, i) => i !== idx));
  };

  const handleAdd = () => {
    setQuestions(prev => [...prev, { question: '', depth_level: 'medium' }]);
  };

  // For dynamic mode, questions are generated during the interview, so no validation needed
  // For predefined mode, check if we have enough questions
  const hasEnoughQuestions = mode === 'dynamic' || questions.length >= questionCount;
  const questionsNeeded = Math.max(0, questionCount - questions.length);

  const handleSave = async () => {
    // Only validate question count for predefined mode
    if (mode === 'predefined' && !hasEnoughQuestions) {
      setError(`Please add ${questionsNeeded} more question${questionsNeeded === 1 ? '' : 's'} to meet the required count of ${questionCount}.`);
      return;
    }

    setLoading(true);
    setError('');
    try {
      const fd = new FormData();
      fd.append('mode', mode);
      fd.append('auto_question_generate', String(autoGenerate));
      
      // For dynamic mode, don't send questions (they're generated during interview)
      // For predefined mode, send the questions
      if (mode === 'predefined') {
        fd.append('manual_questions', JSON.stringify(questions));
      } else {
        // Dynamic mode: no questions to save, they'll be generated during interview
        fd.append('manual_questions', JSON.stringify([]));
      }
      
      if (description) {
        // Update description field (which maps to interview.description or objective)
        fd.append('description', description);
        // Also update objective field
        fd.append('objective', description);
      }

      fd.append('interview_id', interviewId);
      const res = await fetch(`${apiBaseUrl}/api/interview/update-interview`, {
        method: 'POST',
        body: fd,
      });
      
      if (!res.ok) throw new Error('Failed to save');
      onSave();
    } catch (e: any) {
      setError(e.message || 'Failed to save');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
      <div className="bg-white rounded-lg max-w-4xl w-full max-h-[90vh] overflow-y-auto">
        <div className="sticky top-0 bg-white border-b p-4 flex items-center justify-between">
          <div>
            <h2 className="text-xl font-bold">Create Interview</h2>
            <p className="text-sm text-gray-600">We will be using these questions during the interviews. Please make sure they are ok.</p>
          </div>
          <button onClick={onClose} className="p-2 hover:bg-gray-100 rounded">
            <X className="w-5 h-5" />
          </button>
        </div>

        <div className="p-6 space-y-6">
          {error && <div className="p-3 bg-red-50 border border-red-200 rounded text-red-700">{error}</div>}

          {/* Dynamic Mode Info */}
          {mode === 'dynamic' && (
            <div className="p-4 bg-blue-50 border border-blue-200 rounded-lg">
              <h3 className="font-semibold text-blue-900 mb-2">Dynamic Mode Active</h3>
              <p className="text-sm text-blue-800">
                In dynamic mode, questions are generated automatically during the interview based on the candidate's responses. 
                You don't need to create questions here. The system will generate up to {questionCount} questions during the interview, 
                each one building on the candidate's previous answers.
              </p>
            </div>
          )}

          {/* Predefined Mode Content */}
          {mode === 'predefined' && (
            <>
              {autoGenerate && generating && questions.length === 0 && (
                <div className="text-center py-8">Generating questions...</div>
              )}

              {questions.length === 0 && !generating && !autoGenerate && (
                <div className="text-center py-4 text-gray-500">No questions yet. Click + to add.</div>
              )}

              {!hasEnoughQuestions && questions.length > 0 && (
                <div className="p-3 bg-yellow-50 border border-yellow-200 rounded text-yellow-800 text-sm">
                  <strong>Warning:</strong> You need {questionsNeeded} more question{questionsNeeded === 1 ? '' : 's'} to meet the required count of {questionCount}. 
                  Current: {questions.length} / {questionCount}
                </div>
              )}

              {questions.map((q, idx) => (
                <div key={idx} className="border rounded-lg p-4">
                  <div className="flex items-center justify-between mb-2">
                    <span className="font-semibold">Question {idx + 1}</span>
                    <div className="flex items-center gap-4">
                      <div className="flex items-center gap-2">
                        <span className="text-sm">Depth Level:</span>
                        <button
                          onClick={() => handleDepthChange(idx, 'low')}
                          className={`px-3 py-1 rounded text-sm ${q.depth_level === 'low' ? 'bg-purple-600 text-white' : 'bg-gray-200'}`}
                        >
                          Low
                        </button>
                        <button
                          onClick={() => handleDepthChange(idx, 'medium')}
                          className={`px-3 py-1 rounded text-sm ${q.depth_level === 'medium' ? 'bg-purple-600 text-white' : 'bg-gray-200'}`}
                        >
                          Medium
                        </button>
                        <button
                          onClick={() => handleDepthChange(idx, 'high')}
                          className={`px-3 py-1 rounded text-sm ${q.depth_level === 'high' ? 'bg-purple-600 text-white' : 'bg-gray-200'}`}
                        >
                          High
                        </button>
                      </div>
                      <button onClick={() => handleDelete(idx)} className="text-red-600 hover:text-red-800">
                        🗑️
                      </button>
                    </div>
                  </div>
                  <textarea
                    value={q.question}
                    onChange={e => handleQuestionChange(idx, e.target.value)}
                    className="w-full p-3 border rounded resize-y"
                    placeholder="e.g. Can you tell me about a challenging project you've worked on?"
                    rows={3}
                  />
                </div>
              ))}

              <div className="text-center">
                <button
                  onClick={handleAdd}
                  className="px-6 py-3 bg-purple-600 text-white rounded-lg font-semibold text-xl hover:bg-purple-700"
                >
                  +
                </button>
              </div>
            </>
          )}

          <div className="border rounded-lg p-4">
            <h3 className="font-semibold mb-2">Interview Description</h3>
            <p className="text-sm text-gray-600 mb-2">Note: Interviewees will see this description.</p>
            <textarea
              value={description}
              onChange={e => setDescription(e.target.value)}
              className="w-full p-3 border rounded resize-y"
              placeholder="Enter your interview description."
              rows={4}
            />
          </div>

          <div className="flex justify-end gap-3">
            <button
              onClick={onClose}
              className="px-6 py-2 bg-gray-200 text-gray-800 rounded-lg font-semibold hover:bg-gray-300"
            >
              Cancel
            </button>
            <button
              onClick={handleSave}
              disabled={loading || !hasEnoughQuestions}
              className="px-6 py-2 bg-purple-600 text-white rounded-lg font-semibold hover:bg-purple-700 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {loading ? 'Saving...' : mode === 'dynamic' ? 'Continue' : 'Save'}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

