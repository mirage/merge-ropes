open Lwt

module Mem = Irmin_git.AO(Git.Memory)
module Key = Irmin.Hash.SHA1
module Path = Irmin.Path.String_list

module Str = struct
  include Irmin.Contents.String

  type a = char

  let empty = ""
  let length = String.length

  let set t i a =
    let s = Bytes.copy t in
    Bytes.set s i a; s
  let get = String.get

  let insert t i s =
    assert (0 <= i && i <= String.length t);
    let left = String.sub t 0 i in
    let right = String.sub t i (String.length t - i) in
    String.concat "" [left; s; right]

  let delete t i j =
    assert (0 <= i && (i + j) <= String.length t);
    let left = String.sub t 0 i in
    let right = String.sub t (i + j) (String.length t - (i + j)) in
    String.concat "" [left; right]

  let append s t = String.concat "" [s; t]

  let concat sep list = String.concat sep list

  let split t i =
    assert (0 <= i && i <= String.length t);
    let left = String.sub t 0 i in
    let right = String.sub t i (String.length t - i) in
    (left, right)
end

module Config = struct
  let conf = Irmin_git.config ()
  let task = Irmin_unix.task
end

module Rope = Merge_rope.Make(Mem)(Key)(Str)(Config)

module type S = sig
  type state
  type clue

  val create : int -> state * string

  val split : state -> clue -> state * state
  val merge :  state -> state -> state

  val next_clue : state -> (clue * state) option
  val get_str : state -> clue -> string
  val get_len : state -> clue -> int
  val get_pos : state -> clue -> int

  val verify : state -> string -> bool
end

let string_of_char_list l =
  let s = Bytes.create (List.length l) in
  List.iteri (Bytes.set s) l;
  s

let array_permute t =
  let swap i j = let tmp = t.(i) in t.(i) <- t.(j); t.(j) <- tmp in
  for i = Array.length t downto 2 do
    swap (i - 1) (Random.int i)
  done

let list_permute t =
  let a = Array.of_list t in
  array_permute a;
  Array.to_list a

let list_split_n t_orig n =
  if n <= 0 then
    ([], t_orig)
  else
    let rec loop n t accum =
      if n = 0 then
        (List.rev accum, t)
      else
        match t with
        | [] -> (t_orig, []) (* in this case, t_orig = List.rev accum *)
        | hd :: tl -> loop (n - 1) tl (hd :: accum)
    in
    loop n t_orig []

module Oracle : S = struct

  module Map = Map.Make(struct
      type t = int
      let compare = Pervasives.compare
    end)

  type clue = {
    rnk : int;
    pos : int;
    str : String.t;
    len : int;
  }

  type state = {
    list : clue list;
    map : (bool * clue) Map.t;
  }


  let empty_clue = { rnk = 0; pos = 0; str = ""; len = 0 }
  let make_clue rnk pos str = { rnk; pos; str ; len = String.length str }

  let make_state list map = { list; map }


  let make_strs n =
    let char_of_int n pos mask =
      char_of_int (33 + (n land (mask lsl pos)) asr pos)
    in
    let rec make_rec lst = function
      | 0 -> lst
      | n ->
        let bits = Random.bits () in
        let c1 = char_of_int bits 0 63 in
        let c2 = char_of_int bits 6 63 in
        let c3 = char_of_int bits 12 63 in
        let c4 = char_of_int bits 18 63 in
        let c5 = char_of_int bits 24 63 in
        let str = string_of_char_list [c1;c2;c3;c4;c5] in
        let lst = str::lst in
        make_rec lst (n - 1)
    in
    make_rec [] n



(*
     let make_str i c = String.make (i/2 + Random.int i) c
     let make_str i c = String.make (1 + Random.int i) c
     let make_str i c = String.make 1 c

     let make_strs _ =
     let rec make_rec k lim lst map =
     if k < lim then (lst, map)
     else
     let s = make_str 8 (char_of_int k) in
     let lst = s::lst in
     let map = Map.add map (k - lim) s in
     make_rec (k - 1) lim lst map
     in
     (*make_rec 122 97 [] Map.empty*)
  *)


  let fusion list =
    let rec fusion_rec acc = function
      | [] -> List.rev acc
      | [a] -> List.rev (a::acc)
      | a::b::l ->
        if (Random.bool ()) then
          fusion_rec acc ((Str.append a b)::l)
        else
          fusion_rec (a::acc) (b::l)
    in
    fusion_rec [] list

  let select list map =
    let (lst1, lst2) = list_split_n (list_permute list) ((List.length list) * 9 / 10) in
    let map = List.fold_left (fun map c -> Map.add c.rnk (true, c) map ) map lst1 in
    let str = Map.fold (fun key (b, c) str ->
        if b then Printf.sprintf "%s%s" str c.str else str
      ) map ""
    in
    make_state lst2 map, str

  let create n =
    let l = fusion (make_strs n) in
    let _, m = List.fold_left (fun (i, m) str -> i+1, Map.add i str m) (0, Map.empty) l in
    let map = Map.mapi
        (let pos = ref 0 in
         fun key data ->
           let clue = make_clue key (!pos) data in
           pos := (!pos) + (clue.len); (false, clue)
        ) m in
    let list = List.mapi
        (let pos = ref 0 in
         fun i str ->
           let clue = make_clue i (!pos) str in
           pos := (!pos) + (clue.len); clue
        ) l in
    select list map

  let split state clue =
    let f = fun c -> c.pos < clue.pos in
    let (l1, l2) = List.partition f state.list in
    let state1 = make_state l1 state.map in
    let state2 = make_state l2 state.map in
    (state1, state2)

  let merge state1 state2 =
    let f key l r = match l, r with
      | None  , None
      | Some _, None
      | None  , Some _ -> assert false
      | Some (b1, c1), Some (b2, c2) ->
        assert (c1 = c2);
        Some (b1 || b2, c1)
    in
    let map = Map.merge f state1.map state2.map in
    let list = List.filter (fun c -> List.mem c state2.list) state1.list in
    make_state list map

  let next_clue state =
    let list = list_permute state.list in
    let map = state.map in
    match list with
    | [] -> None
    | a::list ->
      let map = Map.add (a.rnk) (true, a) map in
      Some (a, {list; map})

  let get_str state clue = clue.str
  let get_len state clue = clue.len
  let get_pos state clue =
    let map = Map.filter (fun key (b, c) -> b && key < clue.rnk) state.map in
    Map.fold (fun key (b, c) pos -> pos + c.len) map 0

  let verify state string =
    let str =
      Map.fold (fun key(b, c) str -> Printf.sprintf "%s%s" str c.str) state.map ""
    in
    print_endline string;
    print_endline str;
    String.(string = str)

end

let (set_sclk, get_sclk, upd_sclk, set_rclk, get_rclk, upd_rclk) =
  let s_sum = ref 0. in
  let r_sum = ref 0. in
  let s_tmp = ref 0. in
  let r_tmp = ref 0. in
  (
    (fun () -> s_tmp := (Sys.time ())),
    (fun () -> !s_sum),
    (fun () -> s_sum := (!s_sum) +. (Sys.time ()) -. (!s_tmp)),
    (fun () -> r_tmp := (Sys.time ())),
    (fun () -> !r_sum),
    (fun () -> r_sum := (!r_sum) +. (Sys.time ()) -. (!r_tmp))
  )

module OP = Irmin.Merge.OP

let merge state string =
  let rec merge_rec d state old =
    (*Rope.flush old >>= fun sold ->
      print_endline (Printf.sprintf "%i <- %s" d sold);*)
    match Oracle.next_clue state with
    | None ->  OP.ok old
    | Some (clue, state1) -> (
        let str = Oracle.get_str state1 clue in
        let pos = Oracle.get_pos state1 clue in
        Rope.insert old pos str >>= fun r1 ->
        (*Rope.flush r1 >>= fun str1 ->
          print_endline (Printf.sprintf "%i <- %s <- %s" d str str1);*)
        match Oracle.next_clue state with
        | None -> OP.ok r1
        | Some (clue, state2) -> (
            let str = Oracle.get_str state2 clue in
            let pos = Oracle.get_pos state2 clue in
            Rope.insert old pos str >>= fun r2 ->
            (*Rope.flush r2 >>= fun str2 ->
              print_endline (Printf.sprintf "%i <- %s <- %s" d str str2);*)
            let old () = Lwt.return @@ `Ok (Some (Some old)) in
            Rope.merge [] ~old (Some r1) (Some r2) >>= fun res ->
            match res with
            | `Conflict _ | `Ok None -> OP.conflict "merge"
            | `Ok (Some rope) ->
              (*Rope.flush rope >>= fun str ->
                print_endline (Printf.sprintf "%i -> %s" d str);*)
              merge_rec (d + 1) (Oracle.merge state1 state2) rope
          )
      )
  in
  Rope.make string >>= fun rope ->
  merge_rec 0 state rope

let main () =

  let len = 256 in
  let nbr = 64 in
  Random.self_init ();

  let rec iter c = function
    | 0 -> return ()
    | n ->
      Printf.printf "%-8i->      %-8i%s%!" n c "\r";
      let (state, string) = Oracle.create len in
      merge state string >>= fun res ->
      match res with
      | `Conflict s -> iter (c + 1) (n - 1)
      | `Ok _ -> iter c (n - 1)
      (*Rope.flush rope >>= fun str ->
        assert (Oracle.verify state str);*)
  in

  iter 0 nbr >>= fun () ->
  let stats = Rope.stats () in
  print_endline "Merge:";
  print_endline (Merge_rope.string_of_statlist stats.Merge_rope.merge);
  return ()

let () =
  Lwt_unix.run (main ())
