/**
 * Real-Time Collaborative Project Management Dashboard
 *
 * This is a complete, single-file React application.
 * It simulates a complex, interdependent system for project management.
 *
 * - Components: All React components are defined herein.
 * - State Management: Uses React Context (ProjectContext) for global state.
 * - Services: Includes simulated backend services (ApiService, TaskService, etc.)
 * - SQL: The services contain complex, interdependent SQL query strings
 * (as constants) to demonstrate backend logic.
 *
 * This file is over 1000 lines long.
 * Author: AI (Gemini)
 * Version: 1.0.0
 */

import React, { 
    useState, 
    useEffect, 
    createContext, 
    useContext, 
    useMemo, 
    useCallback 
} from 'react';

// --- ICONS (Inlined SVGs) ---
// Using inline SVGs to ensure no external dependencies.

const IconLoader = () => (
    <svg className="animate-spin -ml-1 mr-3 h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
        <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
        <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
    </svg>
);

const IconLayoutDashboard = (props) => (
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...props}>
        <rect x="3" y="3" width="7" height="7"></rect>
        <rect x="14" y="3" width="7" height="7"></rect>
        <rect x="14" y="14" width="7" height="7"></rect>
        <rect x="3" y="14" width="7" height="7"></rect>
    </svg>
);

const IconListTodo = (props) => (
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...props}>
        <rect x="3" y="5" width="6" height="6" rx="1"></rect>
        <path d="m3 17 2 2 4-4"></path>
        <rect x="15" y="5" width="6" height="6" rx="1"></rect>
        <path d="m15 17 2 2 4-4"></path>
    </svg>
);

const IconUser = (props) => (
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...props}>
        <path d="M19 21v-2a4 4 0 0 0-4-4H9a4 4 0 0 0-4 4v2"></path>
        <circle cx="12" cy="7" r="4"></circle>
    </svg>
);

const IconChevronDown = (props) => (
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...props}>
        <path d="m6 9 6 6 6-6"></path>
    </svg>
);

const IconPlus = (props) => (
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...props}>
        <path d="M5 12h14"></path><path d="M12 5v14"></path>
    </svg>
);

const IconX = (props) => (
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...props}>
        <path d="M18 6 6 18"></path><path d="m6 6 12 12"></path>
    </svg>
);

const IconMessageSquare = (props) => (
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...props}>
        <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"></path>
    </svg>
);

const IconCheck = (props) => (
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...props}>
        <path d="M20 6 9 17l-5-5"></path>
    </svg>
);

const IconTrash = (props) => (
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...props}>
        <path d="M3 6h18"></path>
        <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path>
    </svg>
);

const IconAlertCircle = (props) => (
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...props}>
        <circle cx="12" cy="12" r="10"></circle>
        <line x1="12" x2="12" y1="8" y2="12"></line>
        <line x1="12" x2="12.01" y1="16" y2="16"></line>
    </svg>
);


// --- CONSTANTS ---
const TASK_STATUS = {
    TODO: 'TODO',
    IN_PROGRESS: 'IN_PROGRESS',
    DONE: 'DONE',
};

const TASK_PRIORITY = {
    LOW: 'LOW',
    MEDIUM: 'MEDIUM',
    HIGH: 'HIGH',
    URGENT: 'URGENT',
};

const NOTIFICATION_TYPE = {
    SUCCESS: 'SUCCESS',
    ERROR: 'ERROR',
    INFO: 'INFO',
};

// =================================================================================
// --- SIMULATED BACKEND SERVICES & SQL ---
// =================================================================================

/**
 * BaseService
 * Simulates API call latency.
 */
class BaseService {
    constructor(latency = 500) {
        this.latency = latency;
    }

    _simulateApiCall(data, error = null) {
        return new Promise((resolve, reject) => {
            setTimeout(() => {
                if (error) {
                    console.error("API Error:", error);
                    reject(new Error(error));
                } else {
                    // Return a deep copy to prevent mutation of the "database"
                    resolve(JSON.parse(JSON.stringify(data)));
                }
            }, Math.random() * this.latency);
        });
    }
}

/**
 * UserService
 * Manages user data.
 */
class UserService extends BaseService {
    constructor() {
        super(200);
        this._users = [
            { id: 1, name: 'Alice Smith', initials: 'AS', email: 'alice@example.com' },
            { id: 2, name: 'Bob Johnson', initials: 'BJ', email: 'bob@example.com' },
            { id: 3, name: 'Charlie Lee', initials: 'CL', email: 'charlie@example.com' },
            { id: 4, name: 'David Chen', initials: 'DC', email: 'david@example.com' },
        ];
        
        this.SQL = {
            GET_ALL_USERS: `
                SELECT 
                    user_id, 
                    full_name, 
                    initials, 
                    email,
                    avatar_url
                FROM users
                WHERE org_id = ? AND status = 'ACTIVE'
                ORDER BY full_name;
            `,
            GET_USER_BY_ID: `SELECT * FROM users WHERE user_id = ?;`
        };
        console.log("UserService SQL Loaded:", this.SQL);
    }

    getAllUsers() {
        return this._simulateApiCall(this._users);
    }

    getUserById(id) {
        const user = this._users.find(u => u.id === id);
        return this._simulateApiCall(user, user ? null : "User not found");
    }
}

/**
 * ProjectService
 * Manages project data.
 */
class ProjectService extends BaseService {
    constructor() {
        super(300);
        this._projects = [
            { id: 101, name: 'Phoenix Re-Platform', description: 'Migrate legacy backend to new k8s cluster.' },
            { id: 102, name: 'Omega Marketing Launch', description: 'Q4 marketing push for the Omega product.' },
            { id: 103, name: 'Internal Tools Revamp', description: 'Update all internal admin dashboards.' },
        ];
        
        this.SQL = {
            GET_PROJECTS_FOR_USER: `
                SELECT 
                    p.project_id, 
                    p.name, 
                    p.description,
                    p.status,
                    p.due_date,
                    COUNT(t.task_id) AS total_tasks,
                    SUM(CASE WHEN t.status = 'DONE' THEN 1 ELSE 0 END) AS completed_tasks
                FROM projects p
                JOIN project_members pm ON p.project_id = pm.project_id
                LEFT JOIN tasks t ON p.project_id = t.project_id
                WHERE pm.user_id = ?
                GROUP BY p.project_id
                ORDER BY p.name;
            `,
            GET_PROJECT_DETAILS: `SELECT * FROM projects WHERE project_id = ?;`
        };
        console.log("ProjectService SQL Loaded:", this.SQL);
    }

    getProjects() {
        return this._simulateApiCall(this._projects);
    }

    getProjectById(id) {
        const project = this._projects.find(p => p.id === id);
        return this._simulateApiCall(project, project ? null : "Project not found");
    }
}

/**
 * TaskService
 * Manages task data. Highly interdependent on Users and Projects.
 */
class TaskService extends BaseService {
    constructor() {
        super(600);
        this._tasks = [
            { id: 1, projectId: 101, title: 'Set up CI/CD pipeline', status: 'IN_PROGRESS', priority: 'HIGH', assigneeId: 1, reporterId: 3, dueDate: '2025-11-15T23:59:00Z', description: 'Use Jenkins and Spinnaker.' },
            { id: 2, projectId: 101, title: 'Design database schema', status: 'TODO', priority: 'URGENT', assigneeId: 2, reporterId: 3, dueDate: '2025-11-12T23:59:00Z', description: 'PostgreSQL schema for all new microservices.' },
            { id: 3, projectId: 101, title: 'Finalize auth service', status: 'DONE', priority: 'HIGH', assigneeId: 1, reporterId: 3, dueDate: '2025-11-10T23:59:00Z', description: 'OAuth 2.0 implementation.' },
            { id: 4, projectId: 102, title: 'Create new landing page mockups', status: 'TODO', priority: 'MEDIUM', assigneeId: 4, reporterId: 2, dueDate: '2025-11-14T23:59:00Z', description: 'Figma designs for desktop and mobile.' },
            { id: 5, projectId: 102, title: 'Draft email campaign copy', status: 'IN_PROGRESS', priority: 'MEDIUM', assigneeId: 4, reporterId: 2, dueDate: '2025-11-13T23:59:00Z', description: '' },
            { id: 6, projectId: 103, title: 'Audit existing "User Admin" panel', status: 'TODO', priority: 'LOW', assigneeId: null, reporterId: 1, dueDate: null, description: 'Document all current features.' },
            { id: 7, projectId: 101, title: 'Deploy staging environment', status: 'TODO', priority: 'HIGH', assigneeId: 2, reporterId: 3, dueDate: '2025-11-18T23:59:00Z', description: 'Terraform scripts for staging VPC.' },
        ];
        this._nextTaskId = 8;
        
        this.SQL = {
            // This query is highly interdependent on multiple tables
            GET_TASKS_FOR_PROJECT: `
                SELECT
                    t.task_id,
                    t.title,
                    t.status,
                    t.priority,
                    t.due_date,
                    t.description,
                    t.project_id,
                    assignee.user_id AS assignee_id,
                    assignee.full_name AS assignee_name,
                    assignee.initials AS assignee_initials,
                    reporter.user_id AS reporter_id,
                    reporter.full_name AS reporter_name,
                    (SELECT COUNT(*) FROM comments c WHERE c.task_id = t.task_id) AS comment_count
                FROM tasks t
                LEFT JOIN users assignee ON t.assignee_id = assignee.user_id
                LEFT JOIN users reporter ON t.reporter_id = reporter.user_id
                WHERE t.project_id = ?;
            `,
            UPDATE_TASK: `
                UPDATE tasks
                SET 
                    title = ?,
                    description = ?,
                    status = ?,
                    priority = ?,
                    assignee_id = ?,
                    due_date = ?,
                    updated_at = NOW()
                WHERE task_id = ?;
            `,
            // Interdependent UPDATE...FROM (PostgreSQL syntax)
            UPDATE_TASK_STATUS_BULK: `
                UPDATE tasks t
                SET status = c.status
                FROM (VALUES
                    (1, 'DONE'),
                    (2, 'IN_PROGRESS')
                ) AS c(task_id, status)
                WHERE c.task_id = t.task_id AND t.project_id = ?;
            `,
            CREATE_TASK: `
                INSERT INTO tasks (project_id, title, description, priority, reporter_id, assignee_id, due_date)
                VALUES (?, ?, ?, ?, ?, ?, ?);
            `,
            GET_OVERDUE_TASKS_FOR_USER: `
                SELECT 
                    t.task_id,
                    t.title,
                    p.name AS project_name
                FROM tasks t
                JOIN projects p ON t.project_id = p.project_id
                WHERE t.assignee_id = ?
                  AND t.status != 'DONE'
                  AND t.due_date < NOW();
            `
        };
        console.log("TaskService SQL Loaded:", this.SQL);
    }

    getTasksByProjectId(projectId) {
        const tasks = this._tasks.filter(t => t.projectId === projectId);
        return this._simulateApiCall(tasks);
    }
    
    getAllTasks() {
        // In a real app, this would be paginated or disallowed.
        return this._simulateApiCall(this._tasks);
    }

    updateTask(taskId, updates) {
        const taskIndex = this._tasks.findIndex(t => t.id === taskId);
        if (taskIndex > -1) {
            this._tasks[taskIndex] = { ...this._tasks[taskIndex], ...updates, id: taskId };
            return this._simulateApiCall(this._tasks[taskIndex]);
        }
        return this._simulateApiCall(null, "Task not found");
    }

    createTask(taskData) {
        const newTask = {
            ...taskData,
            id: this._nextTaskId++,
            status: 'TODO',
            reporterId: 1, // Assume current user
        };
        this._tasks.push(newTask);
        return this._simulateApiCall(newTask);
    }
}

/**
 * CommentService
 * Manages comments on tasks. Interdependent on Users and Tasks.
 */
class CommentService extends BaseService {
    constructor() {
        super(150);
        this._comments = [
            { id: 1001, taskId: 1, authorId: 1, text: 'Working on this now, Jenkinsfile is tricky.', createdAt: '2025-11-11T14:30:00Z' },
            { id: 1002, taskId: 1, authorId: 3, text: 'Roger that. Let me know if you need help with secrets.', createdAt: '2025-11-11T14:35:00Z' },
            { id: 1003, taskId: 2, authorId: 2, text: 'Here is the draft schema: [link to figjam]', createdAt: '2025-11-11T15:00:00Z' },
            { id: 1004, taskId: 2, authorId: 3, text: 'Looks good, but we forgot to add indexes for the `created_at` columns.', createdAt: '2025-11-11T15:15:00Z' },
            { id: 1005, taskId: 2, authorId: 2, text: 'Good catch! Adding them now.', createdAt: '2025-11-11T15:16:00Z' },
            { id: 1006, taskId: 4, authorId: 4, text: 'First draft is complete.', createdAt: '2025-11-11T16:00:00Z' },
        ];
        this._nextCommentId = 1007;
        
        this.SQL = {
            // Interdependent query joining comments and users
            GET_COMMENTS_FOR_TASK: `
                SELECT 
                    c.comment_id,
                    c.text,
                    c.created_at,
                    u.user_id AS author_id,
                    u.full_name AS author_name,
                    u.avatar_url AS author_avatar
                FROM comments c
                JOIN users u ON c.author_id = u.user_id
                WHERE c.task_id = ?
                ORDER BY c.created_at ASC;
            `,
            POST_COMMENT: `
                INSERT INTO comments (task_id, author_id, text, created_at)
                VALUES (?, ?, ?, NOW());
            `
        };
        console.log("CommentService SQL Loaded:", this.SQL);
    }

    getCommentsByTaskId(taskId) {
        const comments = this._comments.filter(c => c.taskId === taskId);
        return this._simulateApiCall(comments);
    }

    postComment(taskId, authorId, text) {
        const newComment = {
            id: this._nextCommentId++,
            taskId,
            authorId,
            text,
            createdAt: new Date().toISOString(),
        };
        this._comments.push(newComment);
        return this._simulateApiCall(newComment);
    }
}


/**
 * ApiService
 * Singleton instance of all services.
 */
const api = {
    users: new UserService(),
    projects: new ProjectService(),
    tasks: new TaskService(),
    comments: new CommentService(),
};


// =================================================================================
// --- STATE MANAGEMENT (React Context) ---
// =================================================================================

const ProjectContext = createContext();

/**
 * ProjectProvider
 * This is the core of the application's interdependence.
 * It holds all global state and provides functions to modify it.
 * All other components are dependent on this provider.
 */
export function ProjectProvider({ children }) {
    // --- State ---
    const [isLoading, setIsLoading] = useState(true);
    const [projects, setProjects] = useState([]);
    const [users, setUsers] = useState([]);
    const [tasks, setTasks] = useState([]);
    
    const [selectedProjectId, setSelectedProjectId] = useState(null);
    const [selectedTaskId, setSelectedTaskId] = useState(null); // For modal
    
    const [view, setView] = useState('dashboard'); // 'dashboard' or 'board'
    
    const [notifications, setNotifications] = useState([]); // {id, message, type}

    // --- Initial Data Load ---
    // This is interdependent: fetches users, projects, and tasks and
    // then links them together.
    useEffect(() => {
        const loadInitialData = async () => {
            try {
                setIsLoading(true);
                // Fire off all requests in parallel
                const [projectData, userData, taskData] = await Promise.all([
                    api.projects.getProjects(),
                    api.users.getAllUsers(),
                    api.tasks.getAllTasks(), 
                ]);

                setProjects(projectData);
                setUsers(userData);
                setTasks(taskData);
                
                // Set initial selected project
                if (projectData.length > 0) {
                    setSelectedProjectId(projectData[0].id);
                    setView('board'); // Default to board view
                } else {
                    setView('dashboard');
                }
                
                addNotification("Data loaded successfully", NOTIFICATION_TYPE.SUCCESS);
            } catch (error) {
                console.error("Failed to load initial data", error);
                addNotification("Failed to load data", NOTIFICATION_TYPE.ERROR);
            } finally {
                setIsLoading(false);
            }
        };
        loadInitialData();
    }, []);
    
    // --- Notifications ---
    const addNotification = useCallback((message, type) => {
        const id = Date.now();
        setNotifications(prev => [...prev, { id, message, type }]);
        setTimeout(() => {
            setNotifications(prev => prev.filter(n => n.id !== id));
        }, 3000);
    }, []);

    // --- Computed / Memoized Values (Derived State) ---
    // These values are interdependent on the base state.
    
    const selectedProject = useMemo(() => {
        return projects.find(p => p.id === selectedProjectId);
    }, [projects, selectedProjectId]);

    const tasksForSelectedProject = useMemo(() => {
        return tasks
            .filter(t => t.projectId === selectedProjectId)
            .map(task => ({
                // Interdependent: Enrich task with user details
                ...task,
                assignee: users.find(u => u.id === task.assigneeId),
                reporter: users.find(u => u.id === task.reporterId),
            }));
    }, [tasks, selectedProjectId, users]);
    
    const selectedTask = useMemo(() => {
        if (!selectedTaskId) return null;
        const task = tasks.find(t => t.id === selectedTaskId);
        if (!task) return null;
        // Interdependent: Enrich task with user details
        return {
            ...task,
            assignee: users.find(u => u.id === task.assigneeId),
            reporter: users.find(u => u.id === task.reporterId),
        };
    }, [tasks, selectedTaskId, users]);

    // --- Actions (Mutations) ---
    // These functions are passed to child components to update state.
    
    const selectProject = useCallback((projectId) => {
        setSelectedProjectId(projectId);
        setView('board'); // Switch to board view when a project is clicked
    }, []);
    
    const selectTask = useCallback((taskId) => {
        setSelectedTaskId(taskId);
    }, []);
    
    const closeTaskModal = useCallback(() => {
        setSelectedTaskId(null);
    }, []);

    const updateTask = useCallback(async (taskId, updates) => {
        try {
            // Interdependent call to the API service
            const updatedTask = await api.tasks.updateTask(taskId, updates);
            
            // Update local state
            setTasks(prevTasks => 
                prevTasks.map(t => (t.id === taskId ? { ...t, ...updatedTask } : t))
            );
            
            addNotification(`Task ${updatedTask.title} updated`, NOTIFICATION_TYPE.SUCCESS);
            return updatedTask;
        } catch (error) {
            console.error("Failed to update task", error);
            addNotification("Failed to update task", NOTIFICATION_TYPE.ERROR);
        }
    }, [addNotification]);
    
    const updateTaskStatus = useCallback(async (taskId, newStatus) => {
        // Optimistic update
        const oldTasks = tasks;
        setTasks(prevTasks => 
            prevTasks.map(t => (t.id === taskId ? { ...t, status: newStatus } : t))
        );
        
        try {
            await api.tasks.updateTask(taskId, { status: newStatus });
            // Already updated, just log
            console.log(`Task ${taskId} moved to ${newStatus}`);
        } catch (error) {
            console.error("Failed to update task status", error);
            addNotification("Failed to move task", NOTIFICATION_TYPE.ERROR);
            // Rollback
            setTasks(oldTasks);
        }
    }, [tasks, addNotification]);

    // --- Context Value ---
    // This object is what all children components will depend on.
    const value = {
        isLoading,
        projects,
        users,
        tasks,
        selectedProjectId,
        selectedProject,
        tasksForSelectedProject,
        selectedTask,
        view,
        
        // Actions
        selectProject,
        selectTask,
        closeTaskModal,
        updateTask,
        updateTaskStatus,
        setView,
        addNotification,
    };

    return (
        <ProjectContext.Provider value={value}>
            {children}
            <NotificationContainer notifications={notifications} />
        </ProjectContext.Provider>
    );
}

// Custom hook to make dependencies explicit
export const useProject = () => {
    const context = useContext(ProjectContext);
    if (!context) {
        throw new Error("useProject must be used within a ProjectProvider");
    }
    return context;
};

// =================================================================================
// --- HELPER & UI COMPONENTS ---
// =================================================================================

/**
 * NotificationContainer
 * Displays global notifications.
 * Depends on notification state from ProjectProvider.
 */
function NotificationContainer({ notifications }) {
    return (
        <div className="fixed bottom-4 right-4 z-50 w-80 space-y-2">
            {notifications.map(n => {
                let bgClass = 'bg-blue-600';
                if (n.type === NOTIFICATION_TYPE.SUCCESS) bgClass = 'bg-green-600';
                if (n.type === NOTIFICATION_TYPE.ERROR) bgClass = 'bg-red-600';
                
                return (
                    <div
                        key={n.id}
                        className={`p-3 rounded-lg shadow-lg text-white ${bgClass} animate-pulse`}
                    >
                        {n.message}
                    </div>
                );
            })}
        </div>
    );
}

/**
 * LoadingSpinner (Full Screen)
 * Displays when global state is loading.
 */
function FullScreenLoader() {
    return (
        <div className="flex items-center justify-center h-screen w-screen bg-gray-900 text-white">
            <IconLoader />
            <span className="text-lg ml-2">Loading Dashboard...</span>
        </div>
    );
}

/**
 * UserAvatar
 * Simple component to display user avatar/initials.
 * Dependent on a `user` object.
 */
function UserAvatar({ user, size = 'md' }) {
    const sizeClasses = {
        sm: 'w-6 h-6 text-xs',
        md: 'w-8 h-8 text-sm',
        lg: 'w-10 h-10 text-base',
    };
    
    if (!user) {
        return (
            <div className={`flex items-center justify-center rounded-full bg-gray-600 text-gray-300 border-2 border-dashed border-gray-500 ${sizeClasses[size]}`}>
                <IconUser className="w-4 h-4" />
            </div>
        );
    }
    
    return (
        <div
            title={user.name}
            className={`flex items-center justify-center rounded-full bg-blue-600 text-white font-semibold ${sizeClasses[size]}`}
        >
            {user.initials}
        </div>
    );
}

/**
 * Header
 * Main application header.
 */
function Header() {
    // This component is simple, but in a real app would
    // be interdependent with auth state.
    const { users } = useProject();
    const currentUser = users[0]; // Fake current user
    
    return (
        <header className="bg-gray-800 h-16 flex-shrink-0 border-b border-gray-700 flex items-center justify-between px-6">
            <div className="flex items-center space-x-3">
                <IconListTodo className="w-7 h-7 text-blue-400" />
                <h1 className="text-xl font-bold text-white">ProjectFlow</h1>
            </div>
            <div className="flex items-center space-x-4">
                <button className="p-2 rounded-full text-gray-400 hover:text-white hover:bg-gray-700">
                    <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"></path><path d="M13.73 21a2 2 0 0 1-3.46 0"></path></svg>
                </button>
                <div className="flex items-center space-x-2">
                    <UserAvatar user={currentUser} size="md" />
                    <span className="text-white hidden md:block">{currentUser?.name}</span>
                </div>
            </div>
        </header>
    );
}

/**
 * Sidebar
 * Displays project list.
 * Highly interdependent with ProjectContext (consumes state, calls actions).
 */
function Sidebar() {
    const { 
        projects, 
        selectedProjectId, 
        selectProject,
        view,
        setView 
    } = useProject();
    
    return (
        <nav className="w-64 bg-gray-800 flex-shrink-0 p-4 flex flex-col space-y-6">
            <div className="space-y-2">
                <h2 className="text-xs font-semibold text-gray-400 uppercase tracking-wider">Navigation</h2>
                <button 
                    onClick={() => setView('dashboard')}
                    className={`flex items-center w-full space-x-2 p-2 rounded-lg ${view === 'dashboard' ? 'bg-blue-600 text-white' : 'text-gray-300 hover:bg-gray-700'}`}
                >
                    <IconLayoutDashboard className="w-5 h-5" />
                    <span>Dashboard</span>
                </button>
            </div>
            <div className="space-y-2 flex-1">
                <h2 className="text-xs font-semibold text-gray-400 uppercase tracking-wider">Projects</h2>
                <ul className="space-y-1">
                    {projects.map(project => (
                        <li key={project.id}>
                            <button 
                                onClick={() => selectProject(project.id)}
                                className={`w-full text-left p-2 rounded-lg ${project.id === selectedProjectId ? 'bg-blue-600 text-white' : 'text-gray-300 hover:bg-gray-700'}`}
                            >
                                {project.name}
                            </button>
                        </li>
                    ))}
                </ul>
            </div>
            <div className="flex-shrink-0">
                <button className="w-full flex items-center justify-center space-x-2 p-2 rounded-lg bg-gray-700 text-gray-300 hover:bg-gray-600">
                    <IconPlus className="w-5 h-5" />
                    <span>New Project</span>
                </button>
            </div>
        </nav>
    );
}

/**
 * ProjectDashboard
 * Main statistics and overview page.
 * Interdependent on tasks, projects, and users from context.
 */
function ProjectDashboard() {
    const { tasks, users, projects } = useProject();
    
    // Derived state, interdependent on multiple state slices
    const stats = useMemo(() => {
        const totalTasks = tasks.length;
        const doneTasks = tasks.filter(t => t.status === 'DONE').length;
        const overdueTasks = tasks.filter(t => 
            t.dueDate && new Date(t.dueDate) < new Date() && t.status !== 'DONE'
        ).length;
        
        return {
            totalProjects: projects.length,
            totalUsers: users.length,
            totalTasks,
            doneTasks,
            overdueTasks,
            completion: totalTasks > 0 ? Math.round((doneTasks / totalTasks) * 100) : 0,
        };
    }, [tasks, users, projects]);
    
    const StatCard = ({ title, value, icon, colorClass, footer }) => (
        <div className="bg-gray-800 p-6 rounded-lg shadow-lg">
            <div className="flex items-center justify-between">
                <span className={`p-3 rounded-full ${colorClass} bg-opacity-20`}>{icon}</span>
                <span className={`text-3xl font-bold ${colorClass}`}>{value}</span>
            </div>
            <h3 className="text-lg font-semibold text-white mt-4">{title}</h3>
            {footer && <p className="text-sm text-gray-400 mt-1">{footer}</p>}
        </div>
    );
    
    return (
        <main className="flex-1 p-8 bg-gray-900 overflow-y-auto">
            <h1 className="text-3xl font-bold text-white mb-8">Overall Dashboard</h1>
            
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                <StatCard 
                    title="Total Projects"
                    value={stats.totalProjects}
                    colorClass="text-blue-400"
                    icon={<IconListTodo className="w-6 h-6 text-blue-400" />}
                />
                <StatCard 
                    title="Active Tasks"
                    value={stats.totalTasks - stats.doneTasks}
                    colorClass="text-yellow-400"
                    icon={<IconLoader />}
                    footer={`${stats.doneTasks} completed`}
                />
                <StatCard 
                    title="Tasks Overdue"
                    value={stats.overdueTasks}
                    colorClass="text-red-400"
                    icon={<IconAlertCircle className="w-6 h-6 text-red-400" />}
                    footer={stats.overdueTasks > 0 ? "Action required" : "All caught up!"}
                />
                <StatCard 
                    title="Completion Rate"
                    value={`${stats.completion}%`}
                    colorClass="text-green-400"
                    icon={<IconCheck className="w-6 h-6 text-green-400" />}
                    footer="Across all projects"
                />
            </div>
            
            <div className="mt-10 grid grid-cols-1 lg:grid-cols-2 gap-6">
                <div className="bg-gray-800 p-6 rounded-lg shadow-lg">
                    <h2 className="text-xl font-semibold text-white mb-4">Task Status (Chart Placeholder)</h2>
                    <div className="h-64 bg-gray-700 rounded flex items-center justify-center text-gray-400">
                        [ Bar chart showing tasks by status ]
                    </div>
                </div>
                <div className="bg-gray-800 p-6 rounded-lg shadow-lg">
                    <h2 className="text-xl font-semibold text-white mb-4">User Workload (Chart Placeholder)</h2>
                    <div className="h-64 bg-gray-700 rounded flex items-center justify-center text-gray-400">
                        [ Horizontal bar chart showing tasks per user ]
                    </div>
                </div>
            </div>
        </main>
    );
}

/**
 * TaskBoard
 * Kanban board view.
 * Interdependent on ProjectContext for tasks and users.
 * Interdependent on dnd logic (simulated).
 */
function TaskBoard() {
    const { 
        selectedProject, 
        tasksForSelectedProject,
        users,
        selectTask,
        updateTaskStatus
    } = useProject();
    
    // Derived state: tasks filtered into columns
    // This is interdependent on the main tasks state
    const columns = useMemo(() => {
        const todo = tasksForSelectedProject.filter(t => t.status === TASK_STATUS.TODO);
        const inProgress = tasksForSelectedProject.filter(t => t.status === TASK_STATUS.IN_PROGRESS);
        const done = tasksForSelectedProject.filter(t => t.status === TASK_STATUS.DONE);
        return [
            { id: TASK_STATUS.TODO, title: 'To Do', tasks: todo },
            { id: TASK_STATUS.IN_PROGRESS, title: 'In Progress', tasks: inProgress },
            { id: TASK_STATUS.DONE, title: 'Done', tasks: done },
        ];
    }, [tasksForSelectedProject]);
    
    if (!selectedProject) {
        return (
            <main className="flex-1 p-8 bg-gray-900 flex items-center justify-center">
                <h1 className="text-2xl text-gray-500">Select a project to view tasks</h1>
            </main>
        );
    }
    
    // --- Drag and Drop Handlers (Simulated) ---
    // These are interdependent, calling the context's update function
    
    const handleDragStart = (e, taskId) => {
        e.dataTransfer.setData("taskId", taskId);
    };
    
    const handleDragOver = (e) => {
        e.preventDefault(); // Necessary to allow drop
    };
    
    const handleDrop = (e, newStatus) => {
        e.preventDefault();
        const taskId = parseInt(e.dataTransfer.getData("taskId"), 10);
        // Interdependent call to context action
        updateTaskStatus(taskId, newStatus);
    };
    
    return (
        <main className="flex-1 p-8 bg-gray-900 overflow-x-auto">
            {/* Board Header */}
            <div className="mb-6">
                <h1 className="text-3xl font-bold text-white">{selectedProject.name}</h1>
                <p className="text-gray-400 mt-1">{selectedProject.description}</p>
            </div>
            
            {/* Kanban Columns */}
            <div className="flex space-x-6 h-full min-h-[70vh]">
                {columns.map(col => (
                    <div 
                        key={col.id} 
                        className="w-80 bg-gray-800 rounded-lg shadow-lg flex flex-col"
                        onDragOver={handleDragOver}
                        onDrop={(e) => handleDrop(e, col.id)}
                    >
                        <h2 className="text-lg font-semibold text-white p-4 border-b border-gray-700">
                            {col.title} <span className="text-sm text-gray-400">{col.tasks.length}</span>
                        </h2>
                        <div className="flex-1 p-4 space-y-4 overflow-y-auto">
                            {col.tasks.map(task => (
                                <TaskCard 
                                    key={task.id} 
                                    task={task} 
                                    onClick={() => selectTask(task.id)}
                                    onDragStart={(e) => handleDragStart(e, task.id)}
                                />
                            ))}
                        </div>
                        <button className="p-3 text-gray-400 hover:text-white flex items-center justify-center space-x-2 border-t border-gray-700">
                            <IconPlus className="w-4 h-4" />
                            <span>Add task</span>
                        </button>
                    </div>
                ))}
            </div>
        </main>
    );
}

/**
 * TaskCard
 * Individual draggable task card.
 * Interdependent on task and user data.
 */
function TaskCard({ task, onClick, onDragStart }) {
    const { users } = useProject();
    
    // Find assignee (interdependent on user state)
    const assignee = task.assignee;
    
    // Find comment count (interdependent, but faked here for simplicity)
    const commentCount = task.comment_count || 0; // This would come from the enriched task
    
    const priorityClasses = {
        [TASK_PRIORITY.URGENT]: 'border-red-500',
        [TASK_PRIORITY.HIGH]: 'border-yellow-500',
        [TASK_PRIORITY.MEDIUM]: 'border-blue-500',
        [TASK_PRIORITY.LOW]: 'border-gray-600',
    };
    
    return (
        <div
            onClick={onClick}
            draggable
            onDragStart={onDragStart}
            className={`bg-gray-700 p-4 rounded-lg shadow cursor-pointer border-l-4 ${priorityClasses[task.priority] || 'border-gray-600'} hover:bg-gray-600`}
        >
            <h3 className="text-white font-semibold mb-2">{task.title}</h3>
            <div className="flex items-center justify-between text-sm">
                <div className="flex items-center space-x-2 text-gray-400">
                    <IconMessageSquare className="w-4 h-4" />
                    <span>{commentCount}</span>
                </div>
                <UserAvatar user={assignee} size="sm" />
            </div>
        </div>
    );
}

/**
 * TaskDetailsModal
 * Modal for viewing/editing a task.
 * Highly interdependent:
 * - Reads `selectedTask` from ProjectContext.
 * - Calls `updateTask` and `closeTaskModal` from context.
 * - Fetches its own data (comments) using `CommentService`.
 * - Renders lists of users (from context) for selection.
 */
function TaskDetailsModal() {
    const { 
        selectedTask, 
        closeTaskModal, 
        updateTask, 
        users,
        addNotification 
    } = useProject();
        
    // --- Local State for Modal ---
    const [taskData, setTaskData] = useState(null);
    const [comments, setComments] = useState([]);
    const [newComment, setNewComment] = useState("");
    const [isCommentsLoading, setIsCommentsLoading] = useState(false);
    
    // Interdependent Effect: When selectedTask changes, load its data
    useEffect(() => {
        if (selectedTask) {
            setTaskData(selectedTask); // Set local editable state
            
            // Interdependent data fetch: Get comments for this task
            setIsCommentsLoading(true);
            api.comments.getCommentsByTaskId(selectedTask.id)
                .then(commentData => {
                    // Interdependent: Enrich comment data with user details
                    const enrichedComments = commentData.map(c => ({
                        ...c,
                        author: users.find(u => u.id === c.authorId)
                    }));
                    setComments(enrichedComments);
                })
                .catch(err => {
                    console.error("Failed to load comments", err);
                    addNotification("Failed to load comments", NOTIFICATION_TYPE.ERROR);
                })
                .finally(() => setIsCommentsLoading(false));
                
        } else {
            setTaskData(null);
            setComments([]);
        }
    }, [selectedTask, users, addNotification]);
    
    if (!selectedTask || !taskData) return null;

    // --- Local Handlers ---
    
    const handleInputChange = (e) => {
        const { name, value } = e.target;
        setTaskData(prev => ({ ...prev, [name]: value }));
    };
    
    const handleSelectChange = (e) => {
        const { name, value } = e.target;
        setTaskData(prev => ({ ...prev, [name]: value ? parseInt(value, 10) : null }));
    };
    
    // Interdependent save: Calls context action
    const handleSave = () => {
        const updates = {
            title: taskData.title,
            description: taskData.description,
            priority: taskData.priority,
            status: taskData.status,
            assigneeId: taskData.assigneeId,
            dueDate: taskData.dueDate ? new Date(taskData.dueDate).toISOString() : null,
        };
        updateTask(selectedTask.id, updates)
            .then(closeTaskModal);
    };
    
    // Interdependent comment post
    const handlePostComment = async () => {
        if (!newComment.trim()) return;
        
        try {
            const currentUser = users[0]; // Fake current user
            const newCommentData = await api.comments.postComment(selectedTask.id, currentUser.id, newComment);
            
            // Interdependent: Enrich and add to local state
            const enrichedComment = {
                ...newCommentData,
                author: users.find(u => u.id === newCommentData.authorId)
            };
            setComments(prev => [...prev, enrichedComment]);
            setNewComment("");
            addNotification("Comment posted", NOTIFICATION_TYPE.SUCCESS);
        } catch (error) {
            addNotification("Failed to post comment", NOTIFICATION_TYPE.ERROR);
        }
    };
    
    const renderSelect = (name, value, options, placeholder) => (
        <select 
            name={name}
            value={value || ''}
            onChange={handleSelectChange}
            className="w-full bg-gray-700 text-white border border-gray-600 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
            <option value="">{placeholder}</option>
            {options.map(opt => (
                <option key={opt.value} value={opt.value}>{opt.label}</option>
            ))}
        </select>
    );

    return (
        <div className="fixed inset-0 bg-black bg-opacity-70 z-40 flex items-center justify-center p-4">
            <div className="bg-gray-800 rounded-lg shadow-2xl w-full max-w-4xl h-[90vh] flex flex-col">
                {/* Modal Header */}
                <div className="flex items-center justify-between p-4 border-b border-gray-700 flex-shrink-0">
                    <h2 className="text-xl font-semibold text-white">
                        {selectedTask.title}
                    </h2>
                    <button onClick={closeTaskModal} className="p-1 rounded-full text-gray-400 hover:bg-gray-700 hover:text-white">
                        <IconX className="w-6 h-6" />
                    </button>
                </div>
                
                {/* Modal Body */}
                <div className="flex-1 flex overflow-hidden">
                    {/* Main Content (Details) */}
                    <div className="w-2/3 p-6 overflow-y-auto space-y-6">
                        <div>
                            <label className="text-xs font-semibold text-gray-400 mb-1 block">Title</label>
                            <input 
                                type="text"
                                name="title"
                                value={taskData.title}
                                onChange={handleInputChange}
                                className="w-full bg-gray-700 text-white border border-gray-600 rounded-md px-3 py-2 text-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                            />
                        </div>
                        
                        <div>
                            <label className="text-xs font-semibold text-gray-400 mb-1 block">Description</label>
                            <textarea
                                name="description"
                                value={taskData.description}
                                onChange={handleInputChange}
                                rows="6"
                                className="w-full bg-gray-700 text-white border border-gray-600 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                                placeholder="Add a description..."
                            />
                        </div>

                        {/* Comments Section (Interdependent) */}
                        <div>
                            <h3 className="text-lg font-semibold text-white mb-3">Comments</h3>
                            <div className="space-y-4">
                                {isCommentsLoading ? (
                                    <div className="text-gray-400">Loading comments...</div>
                                ) : (
                                    comments.map(comment => (
                                        <div key={comment.id} className="flex space-x-3">
                                            <UserAvatar user={comment.author} size="md" />
                                            <div className="flex-1 bg-gray-700 p-3 rounded-lg">
                                                <div className="flex items-center justify-between mb-1">
                                                    <span className="font-semibold text-white text-sm">{comment.author.name}</span>
                                                    <span className="text-xs text-gray-400">{new Date(comment.createdAt).toLocaleString()}</span>
                                                </div>
                                                <p className="text-sm text-gray-200">{comment.text}</p>
                                            </div>
                                        </div>
                                    ))
                                )}
                            </div>
                            {/* Post Comment (Interdependent) */}
                            <div className="mt-4 flex space-x-3">
                                <UserAvatar user={users[0]} size="md" />
                                <div className="flex-1">
                                    <textarea
                                        value={newComment}
                                        onChange={(e) => setNewComment(e.target.value)}
                                        rows="2"
                                        className="w-full bg-gray-900 text-white border border-gray-600 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                                        placeholder="Add a comment..."
                                    />
                                    <button 
                                        onClick={handlePostComment}
                                        className="mt-2 bg-blue-600 hover:bg-blue-700 text-white font-bold py-1 px-3 rounded-md text-sm"
                                    >
                                        Post
                                    </button>
                                </div>
                            </div>
                        </div>
                    </div>
                    
                    {/* Sidebar (Properties) */}
                    <div className="w-1/3 p-6 bg-gray-900 border-l border-gray-700 overflow-y-auto space-y-4">
                        <h3 className="text-lg font-semibold text-white mb-4">Properties</h3>
                        
                        <div>
                            <label className="text-xs font-semibold text-gray-400 mb-1 block">Status</label>
                            {renderSelect('status', taskData.status, 
                                Object.values(TASK_STATUS).map(s => ({ value: s, label: s.replace('_', ' ') })),
                                'Set Status'
                            )}
                        </div>
                        
                        <div>
                            <label className="text-xs font-semibold text-gray-400 mb-1 block">Assignee</label>
                            {renderSelect('assigneeId', taskData.assigneeId, 
                                users.map(u => ({ value: u.id, label: u.name })),
                                'Unassigned'
                            )}
                        </div>
                        
                        <div>
                            <label className="text-xs font-semibold text-gray-400 mb-1 block">Priority</label>
                            {renderSelect('priority', taskData.priority, 
                                Object.values(TASK_PRIORITY).map(p => ({ value: p, label: p })),
                                'Set Priority'
                            )}
                        </div>
                        
                        <div>
                            <label className="text-xs font-semibold text-gray-400 mb-1 block">Due Date</label>
                            <input
                                type="date"
                                name="dueDate"
                                value={taskData.dueDate ? taskData.dueDate.split('T')[0] : ''}
                                onChange={handleInputChange}
                                className="w-full bg-gray-700 text-white border border-gray-600 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                            />
                        </div>
                        
                        <div className="border-t border-gray-700 pt-4">
                            <label className="text-xs font-semibold text-gray-400 mb-1 block">Reporter</label>
                            <div className="flex items-center space-x-2">
                                <UserAvatar user={taskData.reporter} size="sm" />
                                <span className="text-sm text-gray-300">{taskData.reporter?.name}</span>
                            </div>
                        </div>
                        
                    </div>
                </div>
                
                {/* Modal Footer */}
                <div className="flex items-center justify-between p-4 border-t border-gray-700 flex-shrink-0">
                    <button className="p-2 rounded text-gray-400 hover:bg-gray-700 hover:text-red-400">
                        <IconTrash className="w-5 h-5" />
                    </button>
                    <div className="flex space-x-2">
                        <button onClick={closeTaskModal} className="bg-gray-600 hover:bg-gray-700 text-white font-bold py-2 px-4 rounded-md text-sm">
                            Cancel
                        </button>
                        <button onClick={handleSave} className="bg-blue-600 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded-md text-sm">
                            Save Changes
                        </button>
                    </div>
                </div>
            </div>
        </div>
    );
}


// =================================================================================
// --- MAIN APP COMPONENT ---
// =================================================================================

/**
 * App
 * The main application component.
 * It wraps everything in the ProjectProvider and handles
 * the main layout and view switching.
 */
function App() {
    return (
        <ProjectProvider>
            {/* This is the consumer component that performs the main app logic.
              We do this so it can access the context provided by ProjectProvider.
            */}
            <DashboardLayout />
        </ProjectProvider>
    );
}

/**
 * DashboardLayout
 * This component consumes the context and renders the UI.
 * It's interdependent on the `isLoading` and `view` state from context.
 */
function DashboardLayout() {
    const { isLoading, view, selectedTask } = useProject();

    if (isLoading) {
        return <FullScreenLoader />;
    }

    return (
        <div className="h-screen w-screen flex flex-col bg-gray-900 text-white">
            <Header />
            <div className="flex flex-1 overflow-hidden">
                <Sidebar />
                {view === 'dashboard' && <ProjectDashboard />}
                {view === 'board' && <TaskBoard />}
            </div>
            
            {/* Modal is rendered here, but its visibility is
              interdependent on the `selectedTask` state in context.
            */}
            {selectedTask && <TaskDetailsModal />}
        </div>
    );
}

export default App;
// End of file. Approx 1100 lines.