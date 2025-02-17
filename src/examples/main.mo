import Debug "mo:base/Debug";
import Nat32 "mo:base/Nat32";
import Array "mo:base/Array";
import Int "mo:base/Int";
import Nat "mo:base/Nat";
import Collection "../database/collection/collection";
import QuerySet "../database/query_set";

actor {
    type PrimaryKey = Nat32;

    type User = {
        name : Text;
    };

    type Project = {
        name : Text;
        description : Text;
        creatorId : PrimaryKey;
    };

    type Task = {
        title : Text;
        description : Text;
        completed : Bool;
        projectId : ?PrimaryKey;
    };

    type TaskAssignment = {
        taskId : PrimaryKey;
        userId : PrimaryKey;
        assignmentType : { #owner };
    };

    let users = Collection.Collection<User>();
    let projects = Collection.Collection<Project>();
    let tasks = Collection.Collection<Task>();
    let taskAssignments = Collection.Collection<TaskAssignment>();

    // Add a user to the database

    func addUser(user : User) : () {
        let _result = Collection.add(users, user);
    };

    func addProject(project : Project) : () {
        let _result = Collection.add(projects, project);
    };

    func addTask(task : Task) : () {
        let _taskId = Collection.add(tasks, task);

        // TODO: This should be handled by a foreign key index
        switch (task.projectId) {
            case (null) {};
            case (?projectId) {
                let res = Collection.find(projects, projectId);
                switch (res) {
                    case (null) {
                        Debug.trap("Project with id not found:" # debug_show projectId);
                    };
                    case (?_) {};
                };
            };
        };
    };

    func assignTask(taskId : PrimaryKey, userId : PrimaryKey) : () {
        switch (Collection.find(tasks, taskId)) {
            case (null) {
                Debug.trap("Task with id not found:" # debug_show taskId);
            };
            case (?_) {};
        };

        switch (Collection.find(users, userId)) {
            case (null) {
                Debug.trap("Task with id not found:" # debug_show taskId);
            };
            case (?_) {};
        };

        let assignment = {
            taskId = taskId;
            userId = userId;
            assignmentType = #owner;
        };
        let _ = Collection.add(taskAssignments, assignment);
    };

    func getUsersAssignedToTask(taskId : PrimaryKey) : [(PrimaryKey, User)] {
        let assignments = Collection.filter<TaskAssignment>(
            taskAssignments,
            func(id, assignment) {
                assignment.taskId == taskId;
            },
        );
        let userIds = Array.map<(PrimaryKey, TaskAssignment), PrimaryKey>(
            assignments,
            func((id, assignment)) {
                assignment.userId;
            },
        );
        let _users = Collection.filter<User>(
            users,
            func(id, user) {
                Array.indexOf<PrimaryKey>(id, userIds, Nat32.equal) != null;
            },
        );

        return _users;
    };
};
