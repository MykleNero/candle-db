import Debug "mo:base/Debug";
import Nat32 "mo:base/Nat32";
import Text "mo:base/Text";
import Collection "../src/collection";

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

    let users = Collection.Collection();
    ignore Collection.addSchemaField(
        users,
        {
            name = "name";
            dataType = #text({ defaultValue = null });
        },
    );

    let projects = Collection.Collection();
    ignore Collection.addSchemaFields(
        projects,
        [
            {
                name = "name";
                dataType = #text({ defaultValue = null });
            },
            {
                name = "description";
                dataType = #text({ defaultValue = null });
            },
            {
                name = "creatorId";
                dataType = #nat32({ defaultValue = null });
            },
        ],
    );

    let tasks = Collection.Collection();
    ignore Collection.addSchemaFields(
        tasks,
        [
            {
                name = "title";
                dataType = #text({ defaultValue = null });
            },
            {
                name = "description";
                dataType = #text({ defaultValue = null });
            },
            {
                name = "completed";
                dataType = #text({ defaultValue = null });
            },
            {
                name = "projectId";
                dataType = #nat32Null({ defaultValue = null });
            },
        ],
    );

    let taskAssignments = Collection.Collection();
    ignore Collection.addSchemaFields(
        taskAssignments,
        [
            {
                name = "taskId";
                dataType = #nat32({ defaultValue = null });
            },
            {
                name = "userId";
                dataType = #nat32({ defaultValue = null });
            },
            {
                name = "assignmentType";
                dataType = #text({ defaultValue = null });
            },
        ],
    );

    func validateForeignKey(collection : Collection.Collection, id : PrimaryKey, errorMsg : Text) : () {
        switch (Collection.find(collection, id)) {
            case (null) { Debug.trap(errorMsg) };
            case (?_) {};
        };
    };

    func addUser(user : User) : () {
        let values = [("name", #text(user.name))];
        ignore Collection.insert(users, values);
    };

    func addProject(project : Project) : () {
        let { creatorId } = project;
        validateForeignKey(users, creatorId, "User with id not found: " # debug_show creatorId);
        let values = [
            ("name", #text(project.name)),
            ("description", #text(project.description)),
            ("creatorId", #nat32(creatorId)),
        ];
        ignore Collection.insert(projects, values);
    };

    func addTask(task : Task) : () {
        switch (task.projectId) {
            case (null) {};
            case (?projectId) {
                validateForeignKey(
                    projects,
                    projectId,
                    "Project with id not found: " # debug_show projectId,
                );
            };
        };
        let values = [
            ("title", #text(task.title)),
            ("description", #text(task.description)),
            ("completed", #bool(task.completed)),
            ("projectId", #nat32Null(task.projectId)),
        ];
        ignore Collection.insert(tasks, values);
    };

    func assignTask(taskId : PrimaryKey, userId : PrimaryKey) : () {
        validateForeignKey(tasks, taskId, "Task with id not found: " # debug_show taskId);
        validateForeignKey(users, userId, "User with id not found: " # debug_show userId);
        let values = [
            ("taskId", #nat32(taskId)),
            ("userId", #nat32(userId)),
            ("assignmentType", #text("owner")),
        ];
        ignore Collection.insert(taskAssignments, values);
    };

    func getUsersAssignedToTask(taskId : PrimaryKey) : [(PrimaryKey, [(Text, Collection.FieldValue)])] {
        let assignments = Collection.filter(
            taskAssignments,
            func(id, assignment) {
                for (field in assignment.vals()) {
                    if (field.0 == "taskId" and field.1 == #nat32(taskId)) {
                        return true;
                    };
                };
                return false;
            },
        );
        let assignedUsers = Collection.filter(
            users,
            func(id, user) {
                for ((_, assignment) in assignments.vals()) {
                    for (field in assignment.vals()) {
                        if (field.0 == "userId") {
                            switch (field.1) {
                                case (#nat32(userId)) { return userId == id };
                                case (_) { return false };
                            };
                        };
                    };
                };
                return false;
            },
        );

        return assignedUsers;
    };
};
