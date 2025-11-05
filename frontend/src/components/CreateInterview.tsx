import { useState, useEffect } from 'react';
import { FileText, Info } from 'lucide-react';
import EditQuestionsModal from './EditQuestionsModal';

interface CreateInterviewProps {
  onInterviewCreated?: (interviewId: string, interviewData: any) => void;
  apiBaseUrl: string;
}

interface Interviewer {
  id: string;
  name: string;
  persona: string | null;
  accent: string | null;
  avatar_url: string | null;
}

export default function CreateInterview({ apiBaseUrl }: CreateInterviewProps) {
  const [formData, setFormData] = useState({
    name: '',
    objective: '',
    question_count: 3,
    difficulty_level: 'medium' as 'low' | 'medium' | 'high',
    interviewer_id: '',
    duration_minutes: 10,
  });
  const [jdFile, setJdFile] = useState<File | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [showModal, setShowModal] = useState(false);
  const [createdInterviewId, setCreatedInterviewId] = useState<string>('');
  const [interviewers, setInterviewers] = useState<Interviewer[]>([]);
  const [loadingInterviewers, setLoadingInterviewers] = useState(true);

  // autoOrManual: 'auto' (generate) or 'manual' (typed)
  const [autoOrManual, setAutoOrManual] = useState<'auto' | 'manual'>('auto');
  // mode for auto mode only
  const [autoMode, setAutoMode] = useState<'predefined' | 'dynamic'>('predefined');

  useEffect(() => {
    loadInterviewers();
  }, [apiBaseUrl]);

  const loadInterviewers = async () => {
    try {
      const res = await fetch(`${apiBaseUrl}/api/interviewer/list-interviewers`).then(r => r.json());
      if (res?.ok) setInterviewers(res.interviewers || []);
    } catch (e) {
      console.error('Failed to load interviewers', e);
    } finally {
      setLoadingInterviewers(false);
    }
  };

  const handleQuestionCountChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const val = Math.max(1, parseInt(e.target.value) || 1);
    setFormData(f => ({...f, question_count: val}));
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError('');

    try {
      const fd = new FormData();
      fd.append('name', formData.name);
      fd.append('objective', formData.objective);
      fd.append('question_count', String(formData.question_count));
      fd.append('difficulty_level', formData.difficulty_level);
      
      // Add duration if provided
      if (formData.duration_minutes && formData.duration_minutes > 0) {
        fd.append('duration_minutes', String(formData.duration_minutes));
      }
      
      // Add interviewer_id if selected
      if (formData.interviewer_id) {
        fd.append('interviewer_id', formData.interviewer_id);
      }
      
      // Create interview without generating questions yet
      fd.append('mode', autoOrManual === 'auto' ? autoMode : 'predefined');
      fd.append('auto_question_generate', String(autoOrManual === 'auto'));
      fd.append('manual_questions', '[]'); // Will be set in modal
      
      if (jdFile) {
        fd.append('jd_file', jdFile);
      }
      
      const response = await fetch(`${apiBaseUrl}/api/interview/create-interview`, {
          method: 'POST',
          body: fd,
        });
      if (!response.ok) throw new Error('Failed to create interview');
      const data = await response.json();
      setCreatedInterviewId(data.id);
      setShowModal(true); // Show modal to edit questions
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  };

  const handleModalSave = () => {
    // Redirect to interviews list after saving
    window.history.pushState({}, '', '/');
    window.dispatchEvent(new PopStateEvent('popstate'));
  };

  // Tab/toggle logic
  const handleTab = (which: 'auto'|'manual') => {
    setAutoOrManual(which);
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-50 flex items-center justify-center p-6">
      <div className="bg-white rounded-2xl shadow-xl max-w-2xl w-full p-8">
        <div className="flex items-center gap-3 mb-8">
          <div className="bg-blue-600 p-3 rounded-xl">
            <FileText className="w-8 h-8 text-white" />
          </div>
          <div>
            <h1 className="text-3xl font-bold text-gray-900">Create an Interview</h1>
            <p className="text-gray-600">Set up your interview session</p>
          </div>
        </div>
        {error && (<div className="mb-6 p-4 bg-red-50 border border-red-200 rounded-lg text-red-700">{error}</div>)}
        <form onSubmit={handleSubmit} className="space-y-6">
          <div>
            <label className="block text-sm font-semibold text-gray-700 mb-3">Select an Interviewer</label>
            {loadingInterviewers ? (
              <div className="text-gray-500">Loading interviewers...</div>
            ) : (
              <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-3">
                {interviewers.map((interviewer) => (
                  <div
                    key={interviewer.id}
                    onClick={() => setFormData({ ...formData, interviewer_id: interviewer.id })}
                    className={`relative border-2 rounded-lg p-3 cursor-pointer transition-all ${
                      formData.interviewer_id === interviewer.id
                        ? 'border-blue-600 bg-blue-50'
                        : 'border-gray-200 hover:border-gray-300'
                    }`}
                  >
                    <div className="flex items-center justify-center mb-2">
                      {interviewer.avatar_url ? (
                        <img
                          src={interviewer.avatar_url}
                          alt={interviewer.name}
                          className="w-12 h-12 rounded-full object-cover"
                        />
                      ) : (
                        <div className="w-12 h-12 rounded-full bg-gray-200 flex items-center justify-center">
                          <span className="text-lg text-gray-500">
                            {interviewer.name.charAt(0).toUpperCase()}
                          </span>
                        </div>
                      )}
                    </div>
                    <div className="text-center">
                      <div className="text-sm font-semibold">{interviewer.name}</div>
                      {interviewer.persona && (
                        <div className="text-xs text-gray-600">{interviewer.persona}</div>
                      )}
                    </div>
                    <button
                      type="button"
                      onClick={(e) => {
                        e.stopPropagation();
                        // Show info popup or navigate to interviewer detail
                      }}
                      className="absolute top-1 right-1 p-1 text-gray-400 hover:text-blue-600 rounded transition-colors"
                      title={`${interviewer.name}${interviewer.accent ? ` - ${interviewer.accent} accent` : ''}`}
                    >
                      <Info className="w-3 h-3" />
                    </button>
                  </div>
                ))}
                {interviewers.length === 0 && (
                  <div className="col-span-full text-center text-gray-500 py-4">
                    No interviewers available. 
                    <a
                      href="/interviewers/create"
                      onClick={(e) => {
                        e.preventDefault();
                        window.history.pushState({}, '', '/interviewers/create');
                        window.dispatchEvent(new PopStateEvent('popstate'));
                      }}
                      className="text-blue-600 hover:underline ml-1"
                    >
                      Create one
                    </a>
                  </div>
                )}
              </div>
            )}
          </div>
          <div className="flex items-center space-x-4 mb-2">
            <button
              type="button"
              className={`flex-1 py-2 text-sm font-semibold rounded-lg border ${autoOrManual === 'auto' ? 'bg-blue-600 text-white border-blue-700' : 'bg-gray-100 text-gray-800 border-gray-300'}`}
              onClick={() => handleTab('auto')}
            >
              Generate Questions
            </button>
            <button
              type="button"
              className={`flex-1 py-2 text-sm font-semibold rounded-lg border ${autoOrManual === 'manual' ? 'bg-blue-600 text-white border-blue-700' : 'bg-gray-100 text-gray-800 border-gray-300'}`}
              onClick={() => handleTab('manual')}
            >
              I'll do it myself
            </button>
          </div>
          <div>
            <label className="block text-sm font-semibold text-gray-700 mb-2">Interview Name</label>
            <input type="text" required value={formData.name} onChange={e => setFormData({ ...formData, name: e.target.value })} className="w-full px-4 py-3 border border-gray-300 rounded-lg" placeholder="e.g., Senior Developer Interview" />
          </div>
          <div>
            <label className="block text-sm font-semibold text-gray-700 mb-2">Objective</label>
            <textarea required value={formData.objective} onChange={e => setFormData({ ...formData, objective: e.target.value })} rows={3} className="w-full px-4 py-3 border border-gray-300 rounded-lg" placeholder="Describe the role requirements and expectations..." />
          </div>
          <div>
            <label className="block text-sm font-semibold text-gray-700 mb-2">Upload JD (optional)</label>
            <input
              type="file"
              accept=".pdf,.docx,.doc,.txt"
              onChange={e => setJdFile(e.target.files ? e.target.files[0] : null)}
              className="w-full px-4 py-3 border border-gray-300 rounded-lg"
            />
            <p className="text-xs text-gray-500 mt-1">If provided, questions are based on summarized JD.</p>
          </div>
          <div className="flex w-full space-x-4">
            <div className="flex-1">
              <label className="block text-sm font-semibold text-gray-700 mb-2">Number of Questions</label>
              <input type="number" min="1" max="20" value={formData.question_count} onChange={handleQuestionCountChange} className="w-full px-4 py-3 border border-gray-300 rounded-lg" />
            </div>
            <div className="flex-1">
              <label className="block text-sm font-semibold text-gray-700 mb-2">Duration (minutes)</label>
              <input 
                type="number" 
                min="1" 
                max="180" 
                value={formData.duration_minutes} 
                onChange={(e) => {
                  const val = Math.max(1, parseInt(e.target.value) || 30);
                  setFormData(f => ({...f, duration_minutes: val}));
                }} 
                className="w-full px-4 py-3 border border-gray-300 rounded-lg" 
                placeholder="30"
              />
            <p className="text-xs text-gray-500 mt-1">Total time limit for the entire interview</p>
          </div>
        </div>
        {autoOrManual === 'auto' && (
            <div>
              <label className="block text-sm font-semibold text-gray-700 mb-2">Mode</label>
              <select value={autoMode} onChange={e => setAutoMode(e.target.value as 'predefined'|'dynamic')} className="w-full px-4 py-3 border border-gray-300 rounded-lg">
                <option value="predefined">Predefined (static set)</option>
                <option value="dynamic">Dynamic (LLM generates as candidate answers)</option>
              </select>
            </div>
          )}
            <div>
            <label className="block text-sm font-semibold text-gray-700 mb-2">Difficulty Level</label>
            <select 
              value={formData.difficulty_level} 
              onChange={e => setFormData({ ...formData, difficulty_level: e.target.value as 'low' | 'medium' | 'high' })} 
              className="w-full px-4 py-3 border border-gray-300 rounded-lg"
            >
              <option value="low">Low (Beginner-friendly, basic concepts)</option>
              <option value="medium">Medium (Intermediate, practical experience)</option>
              <option value="high">High (Advanced, expert-level, complex scenarios)</option>
                  </select>
            <p className="text-xs text-gray-500 mt-1">Questions will be generated at this difficulty level.</p>
            </div>
          <button type="submit" disabled={loading} className="w-full bg-blue-600 text-white py-4 rounded-lg font-semibold hover:bg-blue-700 transition disabled:opacity-50 disabled:cursor-not-allowed shadow-lg hover:shadow-xl">
            {loading ? 'Creating...' : 'Create Interview'}
          </button>
        </form>
      </div>
      {showModal && createdInterviewId && (
        <EditQuestionsModal
          interviewId={createdInterviewId}
          autoGenerate={autoOrManual === 'auto'}
          mode={autoMode}
          questionCount={formData.question_count}
          apiBaseUrl={apiBaseUrl}
          onSave={handleModalSave}
          onClose={() => {
            setShowModal(false);
            handleModalSave(); // Redirect anyway
          }}
        />
      )}
    </div>
  );
}
