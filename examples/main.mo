import Nat32 "mo:base/Nat32";
import Text "mo:base/Text";
import Debug "mo:base/Debug";
import Collection "../src/collection";
import Database "../src/database";

// actor {
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

// Create a new database
let db = Database.Database();

// Set up users Collection
let users = Collection.Collection();
ignore Collection.addSchemaField(
    users,
    {
        name = "name";
        dataType = #text({ defaultValue = null });
    },
);
ignore Database.addCollection(db, "users", users);

// Set up projects Collection
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
ignore Database.addCollection(db, "projects", projects);

// Set up tasks Collection
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
            dataType = #bool({ defaultValue = null });
        },
        {
            name = "projectId";
            dataType = #nat32Null({ defaultValue = null });
        },
    ],
);
ignore Database.addCollection(db, "tasks", tasks);

// Set up taskAssignments Collection
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
ignore Database.addCollection(db, "taskAssignments", taskAssignments);

func addUser(user : User) : () {
    let values = [("name", #text(user.name))];
    ignore Database.insert(db, "users", values);
};

func addProject(project : Project) : () {
    let values = [
        ("name", #text(project.name)),
        ("description", #text(project.description)),
        ("creatorId", #nat32(project.creatorId)),
    ];
    ignore Database.insert(db, "projects", values);
};

func addTask(task : Task) : () {
    let values = [
        ("title", #text(task.title)),
        ("description", #text(task.description)),
        ("completed", #bool(task.completed)),
        ("projectId", #nat32Null(task.projectId)),
    ];
    ignore Database.insert(db, "tasks", values);
};

func assignTask(taskId : PrimaryKey, userId : PrimaryKey) : () {
    let values = [
        ("taskId", #nat32(taskId)),
        ("userId", #nat32(userId)),
        ("assignmentType", #text("owner")),
    ];
    ignore Database.insert(db, "taskAssignments", values);
};

func getUsersAssignedToTask(taskId : PrimaryKey) : [(PrimaryKey, [(Text, Collection.FieldValue)])] {
    let qs = Database.select(
        db,
        "taskAssignments",
        {
            fields = [{
                name = "userId";
                alias = null;
            }];
            where = [{
                field = "taskId";
                comparator = #eq(#nat32(taskId));
            }];
        },
    );

    switch (qs) {
        case (#ok(qs)) {
            qs.join(
                db,
                "users",
                [
                    {
                        field = "userId";
                        comparator = #eq("id");
                    },
                ],
            ).join(
                db,
                "tasks",
                [
                    {
                        field = "taskId";
                        comparator = #eq("id");
                    },
                ],
            ).join(
                db,
                "projects",
                [
                    {
                        field = "tasks.projectId";
                        comparator = #eq("id");
                    },
                ],
            ).value();
        };
        case (#err(_)) {
            [];
        };
    };
};

addUser({ name = "Alice" });
addUser({ name = "Bob" });

addProject({
    name = "Project 1";
    description = "Description 1";
    creatorId = 1;
});

addTask({
    title = "Task 1";
    description = "Description 1";
    completed = false;
    projectId = ?1;
});
addTask({
    title = "Task 2";
    description = "Description 2";
    completed = true;
    projectId = null;
});

assignTask(1, 1);
assignTask(2, 2);

let assigned = getUsersAssignedToTask(1);
Debug.print(debug_show (assigned));

// };
