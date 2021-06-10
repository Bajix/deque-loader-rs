table! {

    content (id) {
        id -> Int4,
        title -> Varchar,
    }
}

table! {

    users (id) {
        id -> Int4,
        name -> Varchar,
    }
}

table! {

    users_content (user_id, content_id) {
        user_id -> Int4,
        content_id -> Int4,
    }
}

joinable!(users_content -> content (content_id));
joinable!(users_content -> users (user_id));

allow_tables_to_appear_in_same_query!(
    content,
    users,
    users_content,
);
